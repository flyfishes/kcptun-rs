//! High-level SMUX connection wrapper for standalone usage.
//!
//! `SmuxConn` wraps a [`Session`] with background read/write/keepalive tasks,
//! so users can just `open_stream()` and use the returned [`SmuxIo`] with
//! standard async I/O — no manual flush loop needed.
//!
//! ## Quick start (simplest)
//!
//! Pass your transport directly — `SmuxConn` takes ownership and drives it
//! in a background task. You don't need to spawn anything.
//!
//! ```ignore
//! use smux_rs::{SmuxConn, Config};
//! use kio::{AsyncReadExt, AsyncWriteExt};
//!
//! // Client: connect and get a ready-to-use multiplexed connection
//! let tcp = kio::TcpStream::connect("127.0.0.1:8080").await?;
//! let conn = SmuxConn::client(Config::default(), tcp)?;
//!
//! let mut stream = conn.open_stream()?;
//! stream.write_all(b"hello").await?;
//! let mut buf = [0u8; 1024];
//! let n = stream.read(&mut buf).await?;
//! ```
//!
//! ```ignore
//! // Server: accept a transport and serve streams
//! let (tcp, _) = listener.accept().await?;
//! let conn = SmuxConn::server(Config::default(), tcp)?;
//!
//! loop {
//!     let mut stream = conn.accept().await?;
//!     kio::spawn_task(async move {
//!         // echo or handle the stream
//!     });
//! }
//! ```
//!
//! ## Advanced: manual driver control
//!
//! If you need to manage the driver task yourself (e.g., custom scheduling,
//! or a transport that is not `Send`), use the lower-level constructors:
//!
//! ```ignore
//! let conn = SmuxConn::new(Config::default(), true)?; // no transport yet
//! let driver = conn.clone();
//! kio::spawn_task(async move { let _ = driver.run(&mut tcp).await; });
//! ```
//!
//! You can also split the transport and use [`spawn`](Self::spawn) for
//! concurrent read/write (lower latency) when the runtime supports it.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;

use crate::frame::Frame;
use crate::io::SmuxIo;
use crate::session::{Config, Session, SessionError};

/// How often the `run()` loop polls for keepalive / reap when idle (ms).
const RUN_POLL_MS: u64 = 10;
/// Keepalive / reap check interval (in poll cycles, ~1s at 10ms each).
const HEALTH_INTERVAL: u32 = 100;
/// Stale stream linger before forced reap.
const REAP_LINGER: Duration = Duration::from_secs(30);

/// A high-level SMUX connection that manages transport I/O internally.
///
/// Wraps a [`Session`] with background read/write/keepalive tasks. Users call
/// [`open_stream`](Self::open_stream) (client) or
/// [`accept`](Self::accept) (server) to get a [`SmuxIo`] that implements
/// `kio::AsyncRead + AsyncWrite` directly — no manual `process_data` /
/// `prepare_outbound_into` loop required.
///
/// ## Two driver modes
///
/// - **[`run`](Self::run)**: single-task, takes `&mut T: AsyncRead + AsyncWrite`.
///   Uses a 10 ms read timeout so flush/keepalive can run between reads.
///   Simplest — just `spawn_task` it and use streams.
///
/// - **[`spawn`](Self::spawn)**: two-task, takes separate read + write halves.
///   Read and write run concurrently for lower latency. Requires splitting
///   the transport (e.g. `tokio::io::split`).
///
/// Both modes handle keepalive, timeout, and zombie stream reaping automatically.
#[derive(Clone)]
pub struct SmuxConn {
    session: Arc<Session>,
    flush_notify: Arc<kio::Notify>,
    /// Set to true once a driver task has been started (via run/spawn or
    /// the convenience client/server constructors). Prevents accidentally
    /// starting a second driver on the same connection.
    driven: Arc<std::sync::atomic::AtomicBool>,
}

impl SmuxConn {
    /// Create a new SMUX connection (no transport yet).
    ///
    /// `is_client = true` → client session (odd stream IDs, can open streams).
    /// `is_client = false` → server session (even stream IDs, can accept streams).
    ///
    /// After creating, call [`run`](Self::run) or
    /// [`spawn`](Self::spawn) to drive the connection.
    ///
    /// This is the low-level constructor for advanced use cases where you
    /// need to manage the driver task yourself. Most users should prefer
    /// [`client`](Self::client) or [`server`](Self::server).
    pub fn new(config: Config, is_client: bool) -> Result<Self, SessionError> {
        let session = Arc::new(if is_client {
            Session::new_client(&config)?
        } else {
            Session::new_server(&config)?
        });
        // Server mode: enable the accept queue so process_data() notifies us.
        if !is_client {
            session.enable_accept();
        }
        let flush_notify = Arc::new(kio::Notify::new());
        Ok(Self {
            session,
            flush_notify,
            driven: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Create a client-mode SMUX connection that owns and drives the transport.
    ///
    /// This is the simplest way to get a multiplexed client connection:
    ///
    /// ```ignore
    /// let tcp = kio::TcpStream::connect("127.0.0.1:8080").await?;
    /// let conn = SmuxConn::client(Config::default(), tcp)?;
    ///
    /// let mut stream = conn.open_stream()?;
    /// stream.write_all(b"hello").await?;
    /// ```
    ///
    /// The connection spawns a background driver task internally. You do not
    /// need to call [`run`](Self::run) or [`spawn`](Self::spawn).
    ///
    /// The transport must be `Send + 'static` because it is moved into the
    /// spawned driver task.
    pub fn client<T>(config: Config, mut transport: T) -> Result<Self, SessionError>
    where
        T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static,
    {
        let conn = Self::new(config, true)?;
        // Mark as driven before spawning to prevent accidental double-drive.
        if conn
            .driven
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            // This should be unreachable for a freshly created conn.
            return Err(SessionError::SessionClosed);
        }
        let driver = conn.clone();
        kio::spawn_task(async move {
            let _ = driver.run(&mut transport).await;
        });
        Ok(conn)
    }

    /// Create a server-mode SMUX connection that owns and drives the transport.
    ///
    /// This is the simplest way to serve multiplexed streams over an accepted
    /// connection:
    ///
    /// ```ignore
    /// let (tcp, _) = listener.accept().await?;
    /// let conn = SmuxConn::server(Config::default(), tcp)?;
    ///
    /// loop {
    ///     let mut stream = conn.accept().await?;
    ///     // handle stream...
    /// }
    /// ```
    ///
    /// The connection spawns a background driver task internally. You do not
    /// need to call [`run`](Self::run) or [`spawn`](Self::spawn).
    ///
    /// The transport must be `Send + 'static` because it is moved into the
    /// spawned driver task.
    pub fn server<T>(config: Config, mut transport: T) -> Result<Self, SessionError>
    where
        T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static,
    {
        let conn = Self::new(config, false)?;
        if conn
            .driven
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Err(SessionError::SessionClosed);
        }
        let driver = conn.clone();
        kio::spawn_task(async move {
            let _ = driver.run(&mut transport).await;
        });
        Ok(conn)
    }

    /// Open a new stream (client side).
    ///
    /// Returns a [`SmuxIo`] implementing `kio::AsyncRead + AsyncWrite`.
    /// A SYN frame is automatically queued and sent by the flush loop —
    /// no manual frame encoding needed.
    pub fn open_stream(&self) -> Result<SmuxIo, SessionError> {
        let stream = self.session.open_stream()?;
        self.session.queue_syn(stream.id());
        Ok(SmuxIo::new(stream, self.flush_notify.clone()))
    }

    /// Accept the next incoming stream (server side).
    ///
    /// Waits until a SYN frame arrives (via `process_data` in the driver
    /// loop) and a new stream is created. Returns a [`SmuxIo`].
    pub async fn accept(&self) -> Result<SmuxIo, SessionError> {
        loop {
            if self.session.is_closed() {
                return Err(SessionError::SessionClosed);
            }
            if let Some(id) = self.session.pop_accepted_stream() {
                let streams = self.session.streams();
                if let Some(stream) = streams.lock().get(&id).cloned() {
                    return Ok(SmuxIo::new(stream, self.flush_notify.clone()));
                }
                // Stream was already reaped — try again.
                continue;
            }
            self.session.accept_notify().notified().await;
        }
    }

    /// Drive the connection with a single transport (simplest mode).
    ///
    /// Runs a read / flush / keepalive / reap loop. Returns when the
    /// transport closes (EOF or error) or keepalive times out.
    ///
    /// **Spawn this as a background task** so you can use streams concurrently:
    ///
    /// ```ignore
    /// let conn = Arc::new(SmuxConn::new(Config::default(), true)?);
    /// let driver = conn.clone();
    /// kio::spawn_task(async move {
    ///     let _ = driver.run(&mut tcp).await;
    /// });
    /// // Use conn.open_stream() here…
    /// ```
    ///
    /// Uses a 10 ms read timeout for responsiveness — sufficient for
    /// standalone use. For high-throughput scenarios, use
    /// [`spawn`](Self::spawn) with split read/write halves.
    pub async fn run<T>(&self, transport: &mut T) -> Result<(), SessionError>
    where
        T: kio::AsyncRead + kio::AsyncWrite + Unpin,
    {
        use kio::{AsyncReadExt, AsyncWriteExt};

        let mut read_buf = vec![0u8; 65536];
        let mut write_buf = BytesMut::with_capacity(65536);
        let mut health: u32 = 0;

        loop {
            if self.session.is_closed() {
                break;
            }

            // ── Read from transport (with short timeout) ──
            match kio::timeout(
                Duration::from_millis(RUN_POLL_MS),
                transport.read(&mut read_buf),
            )
            .await
            {
                Ok(Ok(0)) => break, // EOF
                Ok(Ok(n)) => {
                    self.session.process_data(&read_buf[..n])?;
                }
                Ok(Err(_)) => break,
                Err(_) => {} // timeout — fall through to flush
            }

            // ── Flush outbound (SYN + PSH + FIN + UPD) ──
            write_buf.clear();
            let ver = self.session.version();
            let mut fin_ids = self.session.prepare_outbound_into(&mut write_buf, 65536, ver);

            // Reap zombie streams and append their FINs.
            // Reaped streams are removed from the map by reap_stale_streams.
            // We still encode their FINs here and will mark after successful write
            // for any that prepare also returned (mark is a no-op for already-removed).
            // Collect all ids we encoded in this batch for marking on success.
            if health == 0 {
                let need_fin = self.session.reap_stale_streams(REAP_LINGER);
                for id in &need_fin {
                    Frame::encode_header_into(&mut write_buf, ver, crate::frame::Cmd::Fin, *id, 0);
                }
                // Extend so that on success we attempt to mark everything we put on the wire.
                // For reaped entries, the subsequent mark_fins_sent will no-op (already removed),
                // which is consistent with the reaper contract.
                fin_ids.extend(need_fin);
            }

            if !write_buf.is_empty() {
                if transport.write_all(&write_buf).await.is_err() {
                    break;
                }
                self.session.mark_fins_sent(&fin_ids);
            }

            // ── Keepalive / timeout (throttled) ──
            if health == 0 {
                health = HEALTH_INTERVAL;
                if self.session.is_keepalive_timeout() {
                    self.session.close();
                    break;
                }
                if self.session.check_keepalive() {
                    write_buf.clear();
                    self.session.keepalive_frame().encode(&mut write_buf);
                    let _ = transport.write_all(&write_buf).await;
                    self.session.mark_keepalive_sent();
                }
            }
            health -= 1;
        }

        self.session.close();
        Ok(())
    }

    /// Drive the connection with separate read/write halves (concurrent mode).
    ///
    /// Spawns two background tasks:
    /// - **Read task**: reads from `read` → `session.process_data()`
    /// - **Flush task**: `session.prepare_outbound_into()` → writes to `write`
    ///
    /// More efficient than [`run`](Self::run) because read and write
    /// happen concurrently. The flush task also wakes on
    /// `flush_notify` (set by `SmuxIo::poll_write`) for near-instant flush.
    ///
    /// Split a `kio::TcpStream` using your runtime's split function:
    ///
    /// ```ignore
    /// // tokio
    /// let (read, write) = tokio::io::split(tcp);
    /// conn.spawn(read, write);
    ///
    /// // smol
    /// let (read, write) = smol::io::split(tcp);
    /// conn.spawn(read, write);
    /// ```
    pub fn spawn<R, W>(&self, mut read: R, mut write: W)
    where
        R: kio::AsyncRead + Send + Unpin + 'static,
        W: kio::AsyncWrite + Send + Unpin + 'static,
    {
        use kio::{AsyncReadExt, AsyncWriteExt};

        // ── Read task ──
        let session = self.session.clone();
        kio::spawn_task(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                if session.is_closed() {
                    break;
                }
                match read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let _ = session.process_data(&buf[..n]);
                    }
                }
            }
            session.close();
        });

        // ── Flush + keepalive + reap task ──
        let session = self.session.clone();
        let flush_notify = self.flush_notify.clone();
        kio::spawn_task(async move {
            let mut buf = BytesMut::with_capacity(65536);
            let mut nop_buf = BytesMut::with_capacity(8);
            let mut health: u32 = 0;

            loop {
                if session.is_closed() {
                    break;
                }

                // Wait for notify (stream wrote data) or 10 ms timeout.
                let _ =
                    kio::timeout(Duration::from_millis(RUN_POLL_MS), flush_notify.notified()).await;

                buf.clear();
                let ver = session.version();
                let mut fin_ids = session.prepare_outbound_into(&mut buf, 65536, ver);

                // Keepalive / timeout / reap (throttled ~1s)
                if health == 0 {
                    health = HEALTH_INTERVAL;

                    if session.is_keepalive_timeout() {
                        session.close();
                        break;
                    }
                    if session.check_keepalive() {
                        nop_buf.clear();
                        session.keepalive_frame().encode(&mut nop_buf);
                        let _ = write.write_all(&nop_buf).await;
                        session.mark_keepalive_sent();
                    }

                    // Reap zombie streams and append FIN headers for them.
                    // Include their ids in the mark list so that on successful
                    // write we attempt to mark_fins_sent for everything we put
                    // on the wire this batch (consistent with "can't lose FIN").
                    let need_fin = session.reap_stale_streams(REAP_LINGER);
                    for id in &need_fin {
                        Frame::encode_header_into(
                            &mut buf,
                            ver,
                            crate::frame::Cmd::Fin,
                            *id,
                            0,
                        );
                    }
                    fin_ids.extend(need_fin);
                }
                health -= 1;

                if !buf.is_empty() {
                    // Only mark FINs as sent if the transport actually accepted the bytes.
                    // This preserves the "can't lose FIN" invariant.
                    if write.write_all(&buf).await.is_ok() {
                        session.mark_fins_sent(&fin_ids);
                    }
                }
            }
        });
    }

    /// Get the underlying session (for advanced use).
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Close the connection and all streams.
    pub fn close(&self) {
        self.session.close();
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Cmd, FrameCodec};
    use crate::session::DEFAULT_CONFIG;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use bytes::BytesMut;

    /// Verify that SmuxConn is Clone and shares the same session.
    #[test]
    fn smux_conn_clone_shares_session() {
        let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
        let conn2 = conn.clone();
        assert!(std::ptr::eq(
            conn.session() as *const Session,
            conn2.session() as *const Session,
        ));
    }

    /// Verify that open_stream queues a SYN that prepare_outbound drains.
    #[test]
    fn open_stream_then_prepare_outbound_has_syn() {
        let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
        let stream = conn.open_stream().unwrap();
        let id = stream.id();

        let mut buf = BytesMut::new();
        let _ = conn
            .session()
            .prepare_outbound_into(&mut buf, 65536, conn.session().version());

        // Should contain a SYN frame for our stream
        let mut codec = FrameCodec::new(65536);
        codec.feed(&buf);
        let frame = codec.decode().unwrap();
        assert_eq!(frame.cmd, Cmd::Syn);
        assert_eq!(frame.stream_id, id);
    }

    /// Verify that server accept works with process_data.
    #[test]
    fn server_accept_after_syn() {
        let server = SmuxConn::new(DEFAULT_CONFIG.clone(), false).unwrap();

        // Simulate a SYN frame arriving
        let syn = Frame::new(Cmd::Syn, 0, bytes::Bytes::new());
        let mut buf = Vec::new();
        syn.encode(&mut buf);
        server.session().process_data(&buf).unwrap();

        // The stream should be in accepted_streams
        let accepted = server.session().pop_accepted_stream();
        assert_eq!(accepted, Some(0));
        assert!(server.session().pop_accepted_stream().is_none());
    }

    // ─── Mock transport for run()/spawn() FIN marking tests ──────────────────

    #[cfg(feature = "tokio")]
    struct MockTransport {
        written: Arc<Mutex<BytesMut>>,
        fail_next_write: Arc<AtomicBool>,
    }

    #[cfg(feature = "tokio")]
    impl kio::AsyncRead for MockTransport {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut kio::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            // Never deliver data; causes the 10ms timeout in run() to fire and proceed to flush.
            Poll::Pending
        }
    }

    #[cfg(feature = "tokio")]
    impl kio::AsyncWrite for MockTransport {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.fail_next_write.swap(false, Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock write failure",
                )));
            }
            let mut w = this.written.lock().unwrap();
            w.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A write-only half for testing spawn(). Returns Ok(0) on read side is separate.
    #[cfg(feature = "tokio")]
    struct MockWriteHalf {
        written: Arc<Mutex<BytesMut>>,
        fail_next_write: Arc<AtomicBool>,
    }

    #[cfg(feature = "tokio")]
    impl kio::AsyncWrite for MockWriteHalf {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.fail_next_write.swap(false, Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock write failure",
                )));
            }
            let mut w = this.written.lock().unwrap();
            w.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Reader that blocks forever (never returns) so the read task in spawn()
    /// keeps the session alive while we let the flush task reap and write FINs.
    #[cfg(feature = "tokio")]
    struct BlockingReadHalf;

    #[cfg(feature = "tokio")]
    impl kio::AsyncRead for BlockingReadHalf {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut kio::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    #[cfg(all(test, feature = "tokio"))]
    mod tokio_driver_tests {
        use super::*;

        /// run(): A locally-closed stream that is reaped should cause a FIN to be
        /// encoded and written. After a successful write, we consider FINs "sent".
        #[tokio::test]
        async fn run_reap_encodes_fin_and_marks_only_on_success() {
            let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
            let smux_io = conn.open_stream().unwrap();
            let sid = smux_io.id();

            // Operate on the underlying Stream (via the session map) to simulate
            // a stream that has been locally closed long enough to be reaped.
            if let Some(s) = conn.session().streams().lock().get(&sid).cloned() {
                s.mark_local_closed();
                s.force_local_closed_at(
                    std::time::Instant::now() - std::time::Duration::from_secs(120),
                );
            }

            let written = Arc::new(Mutex::new(BytesMut::new()));
            let fail = Arc::new(AtomicBool::new(false));
            let mut transport = MockTransport {
                written: written.clone(),
                fail_next_write: fail.clone(),
            };

            let driver = conn.clone();
            let handle = kio::spawn_task(async move {
                let _ = driver.run(&mut transport).await;
            });

            // Allow the first read-timeout + flush iteration to happen.
            kio::sleep_ms(30).await;

            // Close to unblock the driver loop.
            conn.close();
            // Give the task a moment to exit.
            kio::sleep_ms(20).await;
            drop(handle);

            let data = written.lock().unwrap().clone();
            assert!(!data.is_empty(), "expected flush bytes to be written");

            let mut codec = FrameCodec::new(65536);
            codec.feed(&data);
            let mut saw_fin = false;
            while let Some(f) = codec.decode() {
                if f.cmd == Cmd::Fin && f.stream_id == sid {
                    saw_fin = true;
                }
            }
            assert!(saw_fin, "expected a FIN frame for the reaped stream id on the wire");

            // The reaper should have removed it from the map.
            assert!(
                conn.session().streams().lock().get(&sid).is_none(),
                "reaped stream should be removed from the session map"
            );
        }

        /// run(): If the transport write fails for the batch containing the reaped FIN,
        /// we must NOT treat the FIN as sent (we break without marking).
        #[tokio::test]
        async fn run_does_not_consider_fin_sent_on_write_failure() {
            let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
            let smux_io = conn.open_stream().unwrap();
            let sid = smux_io.id();

            // Operate on the underlying Stream (via the session map) to simulate
            // a stream that has been locally closed long enough to be reaped.
            if let Some(s) = conn.session().streams().lock().get(&sid).cloned() {
                s.mark_local_closed();
                s.force_local_closed_at(
                    std::time::Instant::now() - std::time::Duration::from_secs(120),
                );
            }

            let written = Arc::new(Mutex::new(BytesMut::new()));
            // Fail the very next write (the one that will carry the reaped FIN).
            let fail = Arc::new(AtomicBool::new(true));
            let mut transport = MockTransport {
                written: written.clone(),
                fail_next_write: fail.clone(),
            };

            let driver = conn.clone();
            let _handle = kio::spawn_task(async move {
                let _ = driver.run(&mut transport).await;
            });

            kio::sleep_ms(30).await;
            conn.close();
            kio::sleep_ms(20).await;

            // Because we failed before extending the buffer in the mock, the FIN should not be present.
            let data = written.lock().unwrap().clone();
            let mut codec = FrameCodec::new(65536);
            codec.feed(&data);
            while let Some(f) = codec.decode() {
                assert!(
                    !(f.cmd == Cmd::Fin && f.stream_id == sid),
                    "FIN should not appear in written bytes on write-failure path"
                );
            }

            // Stream was reaped (removed by the reaper before the failing write).
            assert!(conn.session().streams().lock().get(&sid).is_none());
        }

        /// spawn(): Similar to run — FIN from reap must be written, and only "accepted"
        /// (we only mark on success). We use a dedicated write half and an EOF read half.
        #[tokio::test]
        async fn spawn_reap_encodes_fin_and_marks_only_on_success() {
            let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
            let smux_io = conn.open_stream().unwrap();
            let sid = smux_io.id();

            // Operate on the underlying Stream (via the session map) to simulate
            // a stream that has been locally closed long enough to be reaped.
            if let Some(s) = conn.session().streams().lock().get(&sid).cloned() {
                s.mark_local_closed();
                s.force_local_closed_at(
                    std::time::Instant::now() - std::time::Duration::from_secs(120),
                );
            }

            let written = Arc::new(Mutex::new(BytesMut::new()));
            let fail = Arc::new(AtomicBool::new(false));

            let write_half = MockWriteHalf {
                written: written.clone(),
                fail_next_write: fail.clone(),
            };
            let read_half = BlockingReadHalf;

            // spawn consumes the halves and drives read/write tasks.
            conn.spawn(read_half, write_half);

            // Let the flush task run at least one iteration.
            kio::sleep_ms(30).await;

            conn.close();
            kio::sleep_ms(20).await;

            let data = written.lock().unwrap().clone();
            assert!(!data.is_empty(), "expected flush bytes via spawn write half");

            let mut codec = FrameCodec::new(65536);
            codec.feed(&data);
            let mut saw_fin = false;
            while let Some(f) = codec.decode() {
                if f.cmd == Cmd::Fin && f.stream_id == sid {
                    saw_fin = true;
                }
            }
            assert!(saw_fin, "expected a FIN frame for the reaped stream via spawn");

            assert!(conn.session().streams().lock().get(&sid).is_none());
        }

        /// spawn(): write failure must not cause us to consider the FIN sent.
        #[tokio::test]
        async fn spawn_does_not_consider_fin_sent_on_write_failure() {
            let conn = SmuxConn::new(DEFAULT_CONFIG.clone(), true).unwrap();
            let smux_io = conn.open_stream().unwrap();
            let sid = smux_io.id();

            // Operate on the underlying Stream (via the session map) to simulate
            // a stream that has been locally closed long enough to be reaped.
            if let Some(s) = conn.session().streams().lock().get(&sid).cloned() {
                s.mark_local_closed();
                s.force_local_closed_at(
                    std::time::Instant::now() - std::time::Duration::from_secs(120),
                );
            }

            let written = Arc::new(Mutex::new(BytesMut::new()));
            let fail = Arc::new(AtomicBool::new(true)); // fail the reap flush write

            let write_half = MockWriteHalf {
                written: written.clone(),
                fail_next_write: fail.clone(),
            };
            let read_half = BlockingReadHalf;

            conn.spawn(read_half, write_half);

            kio::sleep_ms(30).await;
            conn.close();
            kio::sleep_ms(20).await;

            let data = written.lock().unwrap().clone();
            let mut codec = FrameCodec::new(65536);
            codec.feed(&data);
            while let Some(f) = codec.decode() {
                assert!(
                    !(f.cmd == Cmd::Fin && f.stream_id == sid),
                    "FIN should not be present after a failing write in spawn"
                );
            }

            assert!(conn.session().streams().lock().get(&sid).is_none());
        }
    }
}
