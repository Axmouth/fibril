use fibril_metrics::TcpStats;
use fibril_wire::frame::Frame;
use futures::{Sink, SinkExt};
use std::{fmt::Display, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

/// Shared writer for client replies, deliveries and owner replication frames.
pub(super) async fn run<S>(
    mut writer: S,
    mut frame_rx_high_prio: mpsc::Receiver<Frame>,
    mut frame_rx_low_prio: mpsc::Receiver<Frame>,
    mut shutdown_rx: oneshot::Receiver<()>,
    metrics: Arc<TcpStats>,
) where
    S: Sink<Frame> + Unpin,
    S::Error: Display,
{
    tracing::debug!("[writer] START");

    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(2));
    ticker.tick().await;

    let mut non_flushed_messages: usize = 0;
    let mut last_flush = Instant::now();
    let mut bytes_queued: usize = 0;
    loop {
        tokio::select! {
            biased;

            // ---- Shutdown signal -----------------------------------------
            _ = &mut shutdown_rx => {
                tracing::debug!("[writer] Received shutdown signal");
                // Drain frames already queued and flush before closing:
                // the final frame is often an error reply (auth denial,
                // rejected HELLO) and losing it to this race would turn
                // a guided failure into a bare disconnect.
                while let Ok(frame) = frame_rx_high_prio.try_recv() {
                    if writer.feed(frame).await.is_err() {
                        break;
                    }
                }
                let _ = writer.flush().await;
                break;
            }

            // ---- Normal write path ---------------------------------------
            Some(frame) = frame_rx_high_prio.recv() => {
                tracing::debug!(
                    "[writer] Writing Frame to tcp socket.. code={}",
                    frame.opcode
                );

                let size = size_of_val(&frame) + frame.payload.len();

                if let Err(err) = writer.feed(frame).await {
                    metrics.error();
                    tracing::warn!("[writer] Error writing to tcp socket : {err}");
                    break;
                } else {
                    metrics.bytes_out(size as u64);
                    non_flushed_messages += 1;
                    bytes_queued += size;

                    if non_flushed_messages >= 32 {
                        let _ = writer.flush().await;
                        non_flushed_messages = 0;
                        last_flush = Instant::now();
                        bytes_queued = 0;
                    }
                }
            }

            Some(frame) = frame_rx_low_prio.recv() => {
                tracing::debug!(
                    "[writer] Writing Frame to tcp socket.. code={}",
                    frame.opcode
                );

                let size = size_of_val(&frame) + frame.payload.len();

                if let Err(err) = writer.feed(frame).await {
                    metrics.error();
                    tracing::error!("[writer] Error writing to tcp socket : {err}");
                    break;
                } else {
                    metrics.bytes_out(size as u64);
                    non_flushed_messages += 1;
                    bytes_queued += size;
                }
            }

            // The tick only exists to flush a sub-window tail of buffered
            // frames, so it stays disabled while nothing is buffered and an
            // idle connection costs no periodic wakeups. When the arm
            // re-enables after a quiet stretch the first tick fires
            // immediately, which just runs the flush check below.
            _ = ticker.tick(), if non_flushed_messages > 0 => {
                // pass
            }

            // ---- Channel closed ------------------------------------------
            else => break,
        }

        // Keep batching while work is queued, but flush a drained tail immediately.
        // A concurrent enqueue is handled on the next iteration; it cannot strand
        // frames already fed to the sink. Count, byte and time limits still bound
        // batches under sustained load.
        if (non_flushed_messages > 0)
            && (non_flushed_messages >= 128
                || bytes_queued >= 1024 * 1024
                || last_flush.elapsed().as_millis() >= 5
                || (frame_rx_high_prio.is_empty() && frame_rx_low_prio.is_empty()))
        {
            if let Err(err) = writer.flush().await {
                metrics.error();
                tracing::warn!("[writer] Error writing to tcp socket : {err}");
                break;
            } else {
                non_flushed_messages = 0;
                last_flush = Instant::now();
                bytes_queued = 0;
            }
        }
    }

    tracing::debug!("[writer] EXIT");
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::{
        io,
        pin::Pin,
        sync::Mutex,
        task::{Context, Poll},
    };
    #[derive(Default)]
    struct State {
        pending: Vec<Frame>,
        batches: Vec<Vec<Frame>>,
        fail_send: bool,
        fail_flush: bool,
    }
    struct TestSink(Arc<Mutex<State>>);
    impl Sink<Frame> for TestSink {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, f: Frame) -> Result<(), io::Error> {
            let mut s = self.0.lock().unwrap();
            if s.fail_send {
                return Err(io::Error::other("injected send failure"));
            }
            s.pending.push(f);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            let mut s = self.0.lock().unwrap();
            if s.fail_flush {
                return Poll::Ready(Err(io::Error::other("injected flush failure")));
            }
            let pending = std::mem::take(&mut s.pending);
            if !pending.is_empty() {
                s.batches.push(pending);
            }
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            self.poll_flush(cx)
        }
    }
    struct Setup {
        hi: mpsc::Sender<Frame>,
        lo: mpsc::Sender<Frame>,
        stop: oneshot::Sender<()>,
        state: Arc<Mutex<State>>,
        metrics: Arc<TcpStats>,
        task: tokio::task::JoinHandle<()>,
    }
    fn frame(id: u64) -> Frame {
        Frame {
            version: 1,
            opcode: 4,
            flags: 0,
            request_id: id,
            payload: Bytes::copy_from_slice(&id.to_le_bytes()),
        }
    }
    fn setup(high: usize, low: usize) -> Setup {
        let (hi, hr) = mpsc::channel(2048);
        let (lo, lr) = mpsc::channel(2048);
        let (stop, sr) = oneshot::channel();
        for i in 0..high {
            hi.try_send(frame(10000 + i as u64)).unwrap();
        }
        for i in 0..low {
            lo.try_send(frame(i as u64)).unwrap();
        }
        let state = Arc::new(Mutex::new(State::default()));
        let metrics = TcpStats::new(60);
        let task = tokio::spawn(run(TestSink(state.clone()), hr, lr, sr, metrics.clone()));
        Setup {
            hi,
            lo,
            stop,
            state,
            metrics,
            task,
        }
    }
    async fn poll_writer() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    fn ids(s: &Setup) -> Vec<u64> {
        s.state
            .lock()
            .unwrap()
            .batches
            .iter()
            .flatten()
            .map(|f| {
                assert_eq!(&f.payload[..], &f.request_id.to_le_bytes());
                f.request_id
            })
            .collect()
    }
    async fn finish(s: Setup) {
        s.stop.send(()).unwrap();
        s.task.await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn low_priority_tail_flushes_without_clock_advance() {
        let s = setup(0, 1);
        poll_writer().await;
        assert_eq!(ids(&s), vec![0]);
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn high_priority_tail_flushes_without_clock_advance() {
        let s = setup(1, 0);
        poll_writer().await;
        assert_eq!(ids(&s), vec![10000]);
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn both_queues_drain_in_priority_order_before_tail_flush() {
        let s = setup(2, 3);
        poll_writer().await;
        assert_eq!(ids(&s), vec![10000, 10001, 0, 1, 2]);
        assert_eq!(s.state.lock().unwrap().batches.len(), 1);
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn busy_queue_keeps_bounded_batches() {
        let s = setup(0, 260);
        poll_writer().await;
        assert_eq!(ids(&s), (0..260).collect::<Vec<_>>());
        assert_eq!(
            s.state
                .lock()
                .unwrap()
                .batches
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![128, 128, 4]
        );
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn next_burst_after_idle_is_not_stranded() {
        let s = setup(0, 1);
        poll_writer().await;
        assert_eq!(ids(&s), vec![0]);
        s.lo.send(frame(1)).await.unwrap();
        poll_writer().await;
        assert_eq!(ids(&s), vec![0, 1]);
        assert_eq!(s.state.lock().unwrap().batches.len(), 2);
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_still_drains_high_priority_final_frames() {
        let s = setup(2, 0);
        s.stop.send(()).unwrap();
        s.task.await.unwrap();
        let state = s.state.lock().unwrap();
        assert_eq!(
            state
                .batches
                .iter()
                .flatten()
                .map(|f| f.request_id)
                .collect::<Vec<_>>(),
            vec![10000, 10001]
        );
    }
    #[tokio::test(start_paused = true)]
    async fn tail_flush_failure_ends_writer_and_records_error() {
        let s = setup(0, 1);
        s.state.lock().unwrap().fail_flush = true;
        poll_writer().await;
        assert!(s.task.is_finished());
        assert_eq!(s.metrics.snapshot().errors_total, 1);
        assert!(ids(&s).is_empty());
        s.task.await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn feed_failure_ends_writer_and_records_error() {
        let s = setup(0, 1);
        s.state.lock().unwrap().fail_send = true;
        poll_writer().await;
        assert!(s.task.is_finished());
        assert_eq!(s.metrics.snapshot().errors_total, 1);
        assert!(ids(&s).is_empty());
        s.task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn busy_high_priority_queue_keeps_its_smaller_batch_limit() {
        let s = setup(70, 0);
        poll_writer().await;
        assert_eq!(ids(&s), (10000..10070).collect::<Vec<_>>());
        assert_eq!(
            s.state
                .lock()
                .unwrap()
                .batches
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![32, 32, 6]
        );
        finish(s).await;
    }
    #[tokio::test(start_paused = true)]
    async fn closed_channels_do_not_lose_the_tail() {
        let s = setup(0, 3);
        drop(s.hi);
        drop(s.lo);
        poll_writer().await;
        // Channel closure does not close the connection: the handler owns
        // shutdown. The tail must already be flushed while shutdown is pending.
        assert_eq!(
            s.state
                .lock()
                .unwrap()
                .batches
                .iter()
                .flatten()
                .map(|f| f.request_id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        s.stop.send(()).unwrap();
        s.task.await.unwrap();
    }
}
