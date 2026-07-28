//! Async I/O wrapper for SMUX streams with optional KCP-level backpressure.
//!
//! `SmuxIo` wraps an `Arc<Stream>` and implements `kio::AsyncRead + AsyncWrite`,
//! making it directly usable with `kio::copy_bidirectional` and similar utilities.
//!
//! ## Modes
//!
//! - **Server mode** (`SmuxIo::new`): no KCP backpressure — the flush loop drains
//!   SMUX send buffers at its own pace. Suitable when the TCP side is the bottleneck.
//!
//! - **Client mode** (`SmuxIo::with_backpressure`): when KCP `wait_send >= snd_wnd`,
//!   `poll_write` returns `Pending` and a background task waits for `write_notify`
//!   (signaled from the UDP-ACK / flush path) before waking the blocked writer.
//!   Prevents the pipe from flooding KCP faster than ACKs drain it.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::stream::{poll_read_into, Stream, StreamError};

/// Optional KCP send-window backpressure configuration.
///
/// When `wait_send >= snd_wnd`, `poll_write` returns `Pending` and arms a
/// background task that waits for `write_notify` (signaled when ACKs drain
/// the KCP send queue). At most one waiter task is armed at a time (`bp_armed`).
struct Backpressure {
    /// Shared counter of KCP `wait_send`, updated by the flush loop.
    wait_send: Arc<AtomicUsize>,
    /// KCP send window size — backpressure threshold.
    snd_wnd: usize,
    /// Signaled from the UDP-ACK path / flush loop when the window drains.
    write_notify: Arc<kio::Notify>,
    /// Ensures at most one backpressure waiter task is armed.
    bp_armed: Arc<AtomicBool>,
}

/// Unified async I/O wrapper around an SMUX stream.
///
/// Implements `kio::AsyncRead + AsyncWrite` for both tokio and smol backends.
/// Use with `kio::copy_bidirectional` to pipe between a TCP connection and an
/// SMUX stream.
pub struct SmuxIo {
    stream: Arc<Stream>,
    /// Wake the flush loop immediately when new data is written.
    flush_notify: Arc<kio::Notify>,
    /// Optional KCP send-window backpressure (client mode).
    backpressure: Option<Backpressure>,
}

impl SmuxIo {
    /// Get the stream ID.
    #[inline]
    pub fn id(&self) -> u32 {
        self.stream.id()
    }

    /// Create a new `SmuxIo` without KCP backpressure (server mode).
    ///
    /// `poll_write` always buffers directly and wakes the flush loop.
    pub fn new(stream: Arc<Stream>, flush_notify: Arc<kio::Notify>) -> Self {
        SmuxIo {
            stream,
            flush_notify,
            backpressure: None,
        }
    }

    /// Create a new `SmuxIo` with KCP send-window backpressure (client mode).
    ///
    /// When `wait_send >= snd_wnd`, `poll_write` returns `Pending` and a
    /// background task waits for `write_notify` before waking the writer.
    pub fn with_backpressure(
        stream: Arc<Stream>,
        flush_notify: Arc<kio::Notify>,
        wait_send: Arc<AtomicUsize>,
        snd_wnd: usize,
        write_notify: Arc<kio::Notify>,
    ) -> Self {
        SmuxIo {
            stream,
            flush_notify,
            backpressure: Some(Backpressure {
                wait_send,
                snd_wnd,
                write_notify,
                bp_armed: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    /// Park until KCP send window has room, then wake the poller.
    ///
    /// Prefers `write_notify` (ACK / flush driven). A short timeout is only a
    /// safety net for the rare lost-wakeup race with `notify_waiters` (which
    /// does not store a permit). At most one waiter task is armed at a time.
    fn arm_backpressure_wake(bp: &Backpressure, cx: &mut Context<'_>) {
        let waker = cx.waker().clone();
        if bp
            .bp_armed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Already armed — re-check after a short yield so a race
            // where the waiter just exited cannot stall forever.
            let bp_armed = bp.bp_armed.clone();
            let wait_send = bp.wait_send.clone();
            let snd_wnd = bp.snd_wnd;
            kio::spawn_task(async move {
                kio::sleep_ms(1).await;
                if wait_send.load(Ordering::Relaxed) < snd_wnd
                    || !bp_armed.load(Ordering::Acquire)
                {
                    // Window has room, or previous waiter finished while still
                    // blocked — wake so we re-enter poll_write and re-arm.
                    waker.wake();
                }
            });
            return;
        }
        let write_notify = bp.write_notify.clone();
        let wait_send = bp.wait_send.clone();
        let snd_wnd = bp.snd_wnd;
        let bp_armed = bp.bp_armed.clone();
        kio::spawn_task(async move {
            loop {
                let _ = kio::timeout(Duration::from_millis(2), write_notify.notified()).await;
                if wait_send.load(Ordering::Relaxed) < snd_wnd {
                    bp_armed.store(false, Ordering::Release);
                    waker.wake();
                    return;
                }
            }
        });
    }

    /// Shared `poll_write` logic for both tokio and smol backends.
    ///
    /// Both backends' `AsyncWrite::poll_write` return `Poll<io::Result<usize>>`,
    /// so this is called directly from both cfg-gated impls.
    #[inline]
    fn do_poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // KCP send-window backpressure (client mode only).
        if let Some(ref bp) = self.backpressure {
            let ws = bp.wait_send.load(Ordering::Relaxed);
            if ws >= bp.snd_wnd {
                Self::arm_backpressure_wake(bp, cx);
                return Poll::Pending;
            }
        }
        match self.stream.write(buf) {
            Ok(n) => {
                // Wake the flush loop immediately so it drains SMUX
                // and sends through KCP without waiting for the timer.
                self.flush_notify.notify_one();
                Poll::Ready(Ok(n))
            }
            Err(StreamError::Closed) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "SMUX stream closed",
            ))),
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "SMUX stream write error",
            ))),
        }
    }
}

// ─── tokio AsyncRead / AsyncWrite ─────────────────────────────────────────────

#[cfg(feature = "tokio")]
impl kio::AsyncRead for SmuxIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut kio::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let space = buf.initialize_unfilled();
        match poll_read_into(&this.stream, cx.waker(), space) {
            Poll::Ready(Ok(0)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tokio")]
impl kio::AsyncWrite for SmuxIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().do_poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        log::debug!(
            "SmuxIo::poll_shutdown: marking stream {} local_closed",
            this.stream.id()
        );
        this.stream.mark_local_closed();
        Poll::Ready(Ok(()))
    }
}

// ─── smol AsyncRead / AsyncWrite ──────────────────────────────────────────────

#[cfg(feature = "smol")]
impl kio::AsyncRead for SmuxIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        poll_read_into(&this.stream, cx.waker(), buf)
    }
}

#[cfg(feature = "smol")]
impl kio::AsyncWrite for SmuxIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().do_poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        log::debug!(
            "SmuxIo::poll_close: marking stream {} local_closed",
            this.stream.id()
        );
        this.stream.mark_local_closed();
        Poll::Ready(Ok(()))
    }
}
