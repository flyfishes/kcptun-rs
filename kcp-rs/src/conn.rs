//! Async KCP connection (`KcpConn`) over a datagram transport.
//!
//! Raw KCP with optional Reed-Solomon FEC. Encryption is **not** inside
//! `KcpConn` — inject it via [`PacketTransport`] (e.g. `CryptoTransport` in
//! kcptun-common). Stack: UDP → decrypt → FEC → KCP (in); reverse outbound.
//!
//! Provides `kio::AsyncRead + AsyncWrite` so upper layers (SMUX, etc.) can
//! treat KCP like a reliable stream.
//!
//! Enable with `--features async` / `async-tokio` (tokio) or `async-smol`.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io;
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::config::{KcpConfig, KcpMode};

enum DialTransport {
    Udp,
    TcpRaw,
}
use crate::fec::{
    fec_expand_packets, fec_kcp_from_recovered, FecDecoder, FecEncoder, FEC_HEADER_SIZE_PLUS_2,
    FEC_TYPE_DATA, FEC_TYPE_PARITY,
};
use crate::kcp::KCP;
use crate::segment::KCP_MAX_FRAG;
use crate::transport::{PacketTransport, MAX_DATAGRAM};
use kio::CancellationToken;

/// FEC header + SIZE field (`fecHeaderSizePlus2` in Go).
const FEC_HDR: usize = FEC_HEADER_SIZE_PLUS_2;

/// Safety-net poll interval for `Notify` waits. The normal wake path fires
/// immediately via `notify_one` (permit-storing) or `notify_waiters` (registered
/// waiters); this longer interval only recovers from a *lost* wake — a
/// `notify_waiters` landing with no waiter registered, a multi-reader permit
/// race, or `close()` racing a `notified()` registration. 10ms instead of the
/// previous 2ms cuts the tokio timer-wheel churn ~5x under saturation while
/// still bounding any lost-wake recovery latency.
const WAIT_FALLBACK_MS: u64 = 10;

/// Idle cap on the flush-loop sleep when the link is completely idle
/// (`wait_send == 0`, no buffered data). `kcp.flush()` already returns the
/// KCP interval (10–40ms) when idle; clamping to 100ms instead of the old 2ms
/// cuts per-idle-connection timer-wheel churn ~50x, matching legacy server
/// `MAX_IDLE_UPDATE_MS`. Busy links stay at 1ms (see flush loop).
const MAX_IDLE_UPDATE_MS: u64 = 100;
/// Active connections with unacknowledged data keep a fine-grained driver
/// deadline. This is scheduling precision only; KCP's protocol interval/RTO
/// fields remain unchanged. Idle connections still park without a timer.
const ACTIVE_UPDATE_MAX_MS: u64 = 10;
/// Keep a recently active connection's flush task armed long enough to avoid
/// repeated cold park/wake cycles on sparse interactive traffic. After one
/// quiet grace interval, a truly idle connection parks without another timer.
const IDLE_PARK_GRACE_MS: u64 = 1_000;

/// Max bytes per `send_to_kcp` call.  Limits KCP mutex hold time by chunking
/// large writes (~49 segments at MSS=1326), matching Go's `Write` pattern where
/// the echo loop's 64KB buffer naturally chunks sends.  The caller's `write_all`
/// loop re-acquires the mutex between chunks, letting the input loop process
/// ACKs and open the send window sooner.
const KCP_SEND_CHUNK: usize = 64 * 1024;

/// Completed messages may be moved from KCP into a small bounded read queue by
/// the input task. This pipelines short bursts with application reads without
/// recreating the old unbounded side queue. Allocation is lazy and the cap is
/// per active connection; data beyond it remains governed by KCP's window.
const READ_PREFETCH_MAX_BYTES: usize = 32 * 1024;
/// Keep object/count overhead bounded for tiny application messages. The byte
/// cap alone would permit tens of thousands of one-byte `Bytes` entries.
const READ_PREFETCH_MAX_MESSAGES: usize = 32;

/// Slot count per `try_recv_batch` drain call in the input loop (matches the
/// listener's recvmmsg batch; the transport fills up to this many per call).
const INPUT_BATCH_GROW: usize = 16;
/// Max datagrams processed per input-loop cycle. Bounds one `feed_inbound_batch`
/// + deferred flush so a high-rate peer cannot starve the worker (v3 §5.4).
const MAX_INPUT_BATCH: usize = 64;

/// Max wire-packet slots retained in the recycled raw-output batch. Bounds the
/// memory held across drains after a pathological giant burst; typical batches
/// (tens of packets) are unaffected.
const MAX_RETAINED_RAW_BATCH: usize = 256;

/// Pending/spare double-buffer for produced wire packets, so draining swaps
/// buffers instead of allocating a fresh `Vec<Bytes>` under the lock. The KCP
/// output callback fills `pending`; a sender swaps `pending`↔`spare` to hand
/// out the batch, sends it, then recycles the (cleared) batch back into
/// `spare` so the next burst accumulates without re-growing from zero.
#[derive(Default)]
struct RawPacketQueue {
    /// Accumulating wire packets (filled by the KCP output callback).
    pending: Vec<Bytes>,
    /// Recycled capacity from the last drained batch.
    spare: Vec<Bytes>,
}

impl RawPacketQueue {
    fn push(&mut self, data: Bytes) {
        self.pending.push(data);
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Hand the accumulated batch to the sender: swap `pending` with `spare`
    /// (the recycled buffer becomes the next accumulation target, inheriting
    /// its capacity) and take the full batch. No allocation under the lock.
    fn drain(&mut self) -> Vec<Bytes> {
        std::mem::swap(&mut self.pending, &mut self.spare);
        std::mem::take(&mut self.spare)
    }

    /// Recycle the drained batch's capacity back into the spare slot, unless
    /// it is pathologically large (keeps retained memory bounded). Callers
    /// invoke this on both success and error paths.
    fn recycle(&mut self, mut batch: Vec<Bytes>) {
        if batch.capacity() <= MAX_RETAINED_RAW_BATCH {
            batch.clear();
            self.spare = batch;
        }
    }
}

// ─── Shared state ─────────────────────────────────────────────────────────────

/// Byte- and entry-accounted receive prefetch queue.
///
/// Keeping the accounting under the same mutex makes capacity checks O(1) and
/// ensures a short-read remainder cannot make a stale byte snapshot exceed the
/// configured prefetch bound.
#[derive(Default)]
struct ReadBuffer {
    queue: VecDeque<Bytes>,
    bytes: usize,
}

impl ReadBuffer {
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }

    fn front(&self) -> Option<&Bytes> {
        self.queue.front()
    }

    fn pop_front(&mut self) -> Option<Bytes> {
        let data = self.queue.pop_front()?;
        self.bytes = self.bytes.saturating_sub(data.len());
        Some(data)
    }

    fn push_front(&mut self, data: Bytes) {
        self.bytes = self.bytes.saturating_add(data.len());
        self.queue.push_front(data);
    }

    fn extend(&mut self, data: impl IntoIterator<Item = Bytes>) {
        for item in data {
            self.bytes = self.bytes.saturating_add(item.len());
            self.queue.push_back(item);
        }
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.bytes = 0;
    }
}

struct KcpConnShared {
    transport: Arc<dyn PacketTransport>,
    kcp: Arc<Mutex<KCP>>,
    // read_buf is a small byte-bounded prefetch queue plus a possible partial
    // message when a caller buffer ends mid-read. Remaining complete messages
    // stay in KCP so rcv_wnd remains the primary flow-control boundary.
    read_buf: Mutex<ReadBuffer>,
    /// Serialize concurrent application readers.
    read_op: Mutex<()>,
    /// FIFO of produced wire packets (ACKs / data / probes). The flush loop is
    /// the ONLY drainer + sender — a single owner keeps wire order = flush
    /// order. Multiple concurrent drainers (write path, input loop, flush loop)
    /// interleaved batches on a FIFO link → receiver `rcv_nxt` gaps → spurious
    /// fastack → retransmit storm (256KB@RPS=500).
    raw_packets: Arc<Mutex<RawPacketQueue>>,
    flush_notify: Arc<kio::Notify>,
    write_notify: Arc<kio::Notify>,
    read_notify: Arc<kio::Notify>,
    read_waker: Mutex<Option<Waker>>,
    write_waker: Mutex<Option<Waker>>,
    wait_send: Arc<AtomicUsize>,
    snd_wnd: AtomicUsize,
    acknodelay: AtomicBool,
    remote_addr: SocketAddr,
    /// When true, use `send_batch` / `send_urgent` (connected). Else `*_to(remote)`.
    connected: bool,
    closed: Arc<AtomicBool>,
    /// Cancels the input loop's socket `recv` on `close()` so a silent peer's
    /// task exits immediately instead of waiting out the 100ms poll tick
    /// (removes the ~10 Hz × idle-connection timer churn).
    cancel_token: CancellationToken,
    /// Adopt the conversation ID from the first decrypted KCP segment.
    adopt_conv: AtomicBool,
    /// When false, no background input-loop task is spawned: an external
    /// driver (Acceptor + Worker sharding) feeds inbound via [`KcpConn::feed_input`].
    background_input: bool,
    /// Last successful inbound or outbound user-data activity (monotonic ms).
    last_activity_ms: AtomicU64,
    /// Send token: when `true`, either the flush loop or an inline writer is
    /// draining `raw_packets` + sending via `send_packets_with_fec().await`.
    /// Prevents wire-interleaving when both try to send concurrently.
    /// Acquired via `compare_exchange(false, true)`; released with `store(false)`.
    is_sending: AtomicBool,
    /// Optional FEC encoder (header_offset=0, matching client/server session layout).
    fec_encoder: Option<Mutex<FecEncoder>>,
    /// Optional FEC decoder.
    fec_decoder: Option<Mutex<FecDecoder>>,

    // ── TcpStream-aligned surface ──
    /// Read timeout in ms (`None` = block indefinitely). Honored by
    /// [`KcpConn::read_shared`], `poll_read`, and [`KcpConn::readable`].
    read_timeout: Mutex<Option<u64>>,
    /// Write timeout in ms (`None` = block indefinitely). Honored by
    /// [`KcpConn::write_all_shared`], `poll_write`, and [`KcpConn::writable`].
    write_timeout: Mutex<Option<u64>>,
    /// Mono-ms deadline for a blocked `poll_read` (checked on the next poll).
    read_deadline: Mutex<Option<u64>>,
    /// Mono-ms deadline for a blocked `poll_write` (checked on the next poll).
    write_deadline: Mutex<Option<u64>>,
    /// Last non-transient I/O error from the background loops, surfaced by
    /// [`KcpConn::take_error`].
    last_error: Mutex<Option<io::Error>>,
    /// Last [`KcpConn::set_nodelay`] bool value, for the [`KcpConn::nodelay`] getter.
    nodelay: AtomicBool,
    /// Write side half-closed via [`KcpConn::shutdown`] / `poll_shutdown`.
    write_closed: AtomicBool,
    /// Read side half-closed via [`KcpConn::shutdown`].
    read_closed: AtomicBool,
    /// Any conv-valid inbound datagram received (drives connect-timeout's
    /// first-packet wait).
    first_inbound: AtomicBool,
}

impl KcpConnShared {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Backpressure has room: the send window is below `snd_wnd` (ACKs
    /// are flowing). Writes return partial counts when the KCP window fills,
    /// so there is no second user-space queue to include in this check.
    fn backpressure_relieved(&self) -> bool {
        self.wait_send.load(Ordering::Relaxed) < self.snd_wnd.load(Ordering::Relaxed)
    }

    fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.cancel_token.cancel();
            self.flush_notify.notify_one();
            self.write_notify.notify_waiters();
            self.read_notify.notify_waiters();
            self.wake_writer();
            if let Some(w) = self.read_waker.lock().take() {
                w.wake();
            }
        }
    }

    fn wake_reader(&self) {
        // `notify_one` stores a permit, so a notify that lands before the
        // reader registers its `notified()` is not lost (notify_waiters would
        // drop it and the reader would stall until its 2ms poll slice).
        self.read_notify.notify_one();
        if let Some(w) = self.read_waker.lock().take() {
            w.wake();
        }
    }

    fn wake_writer(&self) {
        // Wake outside the `write_waker` lock: clone the waker out of the slot
        // (one Arc refcount inc), drop the guard, then wake. The slot keeps
        // its waker, so a re-polling writer's `will_wake` still recognizes
        // itself and does NOT reset the write deadline (a `take()` here would
        // clear it and roll the timeout forward forever). Waking an arbitrary
        // Waker while holding an internal mutex risks re-entrancy and lengthens
        // hold time.
        //
        // Also notify `write_notify` so any task waiting on `notified()` is
        // woken (matches `wake_reader` behavior for consistency).
        let waker = self.write_waker.lock().clone();
        self.write_notify.notify_one();
        if let Some(w) = waker {
            w.wake_by_ref();
        }
    }

    fn drain_raw_packets(&self) -> Vec<Bytes> {
        // Swap `pending`↔`spare` under the lock (no allocation) and hand the
        // accumulated batch to the sender; the recycled buffer becomes the next
        // accumulation target with its capacity preserved.
        self.raw_packets.lock().drain()
    }

    /// Return a drained batch's capacity to the queue for reuse (bounded by
    /// [`MAX_RETAINED_RAW_BATCH`]). Every sender calls this after sending, on
    /// both success and error paths, so a giant burst does not permanently pin
    /// memory and the steady state is allocation-free.
    fn recycle_raw_packets(&self, packets: Vec<Bytes>) {
        self.raw_packets.lock().recycle(packets);
    }

    /// Release the send token and preserve a wake-up for packets queued while
    /// the sender was awaiting UDP I/O. The flush task may have consumed the
    /// original notification while the token was held; without a fresh permit,
    /// those packets wait for the next 10ms KCP timer tick.
    fn finish_sending(&self) {
        self.is_sending.store(false, Ordering::Release);
        if !self.raw_packets.lock().is_empty() {
            self.flush_notify.notify_one();
        }
    }

    async fn send_packets(&self, packets: &[Bytes]) -> io::Result<()> {
        if packets.is_empty() {
            return Ok(());
        }
        if self.connected {
            self.transport.send_batch(packets).await
        } else {
            self.transport
                .send_batch_to(packets, self.remote_addr)
                .await
        }
    }

    /// Send `packets`, FEC-expanding first when an encoder is configured.
    ///
    /// When there is no encoder, sends `packets` directly — avoiding the
    /// per-batch `packets.to_vec()` allocation on the common (non-FEC) path
    /// (P0). FEC output owns a fresh `Vec<Bytes>`, so it must be kept alive
    /// for the duration of the send.
    async fn send_packets_with_fec(&self, packets: &[Bytes]) -> io::Result<()> {
        if let Some(ref enc) = self.fec_encoder {
            let wire = {
                let mut e = enc.lock();
                fec_expand_packets(&mut e, packets, 500)
            };
            self.send_packets(&wire).await
        } else {
            self.send_packets(packets).await
        }
    }

    /// Try to acquire the send token (`is_sending`).  If acquired, drain
    /// `raw_packets` and send them inline.  Returns `true` if the send token
    /// was acquired (caller should NOT notify the flush loop — we handled it).
    /// Returns `false` if another sender holds the token (caller should
    /// `flush_notify.notify_one()` to let the flush loop handle it).
    ///
    /// This eliminates the task-scheduling hop of the notify→wake→drain→send
    /// path on the write hot path, matching Go's synchronous `Write` send.
    async fn try_drain_and_send(&self) -> bool {
        if self
            .is_sending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false; // flush loop is sending — let it handle our packets
        }
        let packets = self.drain_raw_packets();
        if packets.is_empty() {
            self.finish_sending();
            return true; // nothing to send, but we acquired the token (no notify needed)
        }
        if let Err(e) = self.send_packets_with_fec(&packets).await {
            *self.last_error.lock() = Some(e);
        }
        self.recycle_raw_packets(packets);
        self.finish_sending();
        true
    }

    /// Inline `kcp.send` + `kcp.flush` under the KCP lock, matching kcp-go's
    /// `UDPSession.Write`. Returns the number of bytes accepted into the KCP
    /// send window (possibly < `buf.len()` when the window fills mid-buffer
    /// or the per-call chunk cap is reached).
    #[inline]
    fn send_to_kcp(&self, kcp: &mut KCP, buf: &[u8]) -> usize {
        let mss = kcp.mss() as usize;
        // Respect the configured send window for this call, not only at the
        // caller's pre-check.  A single 64 KiB write can span dozens of KCP
        // segments; accepting all of it when only one window slot remains
        // defeats backpressure and creates a latency/RSS spike per writer.
        let queued = kcp.wait_send() as usize;
        let available_segments = self.snd_wnd.load(Ordering::Relaxed).saturating_sub(queued);
        let window_bytes = available_segments.saturating_mul(mss);
        if window_bytes == 0 {
            self.wait_send.store(queued, Ordering::Relaxed);
            return 0;
        }
        let max_chunk = (KCP_MAX_FRAG as usize)
            .saturating_sub(1)
            .saturating_mul(mss)
            .max(mss);
        // Cap total bytes per call to limit KCP mutex hold time.  A 256KB
        // write would otherwise hold the mutex for ~195 kcp.send() iterations
        // + data-only flush, blocking the input loop from processing ACKs.
        // Capping at 64KB (~49 segments) matches Go's Write pattern where the
        // echo loop's 64KB buffer naturally chunks sends.  The caller's
        // write_all loop re-acquires the mutex between chunks.
        let buf = &buf[..buf.len().min(KCP_SEND_CHUNK).min(window_bytes)];
        let mut offset = 0usize;
        while offset < buf.len() {
            let end = (offset + max_chunk).min(buf.len());
            if kcp.send(&buf[offset..end]).is_err() {
                break;
            }
            offset = end;
        }
        if offset > 0 {
            // Data-only: writes must not drain ACKs; inbound processing emits
            // immediate ACKs and the protocol deadline emits delayed ACKs.
            // Pass current to avoid a redundant current_ms() call.
            let current = kcp.current_ms() as u32;
            kcp.flush_data_only_with_current(current);
        }
        let ws = kcp.wait_send() as usize;
        self.wait_send.store(ws, Ordering::Relaxed);
        offset
    }
}

// ─── KcpConn ──────────────────────────────────────────────────────────────────

/// Reliable KCP stream over a datagram transport (`AsyncRead + AsyncWrite`).
///
/// Optional Reed-Solomon FEC when configured via [`.fec(d, p)`](KcpConnBuilder::fec).
/// No encryption — inject via [`PacketTransport`] (e.g. CryptoTransport).
/// Background input/flush loops drive the KCP state machine; user I/O only
/// touches shared buffers + notifies.
pub struct KcpConn {
    shared: Arc<KcpConnShared>,
    _handles: Vec<kio::JoinHandle<()>>,
    /// `true` for the original `KcpConn` created by the builder; `false` for
    /// clones.  Only the owner's `Drop` calls `close()` — clones dropping
    /// do NOT close the connection.
    owns_connection: bool,
}

impl KcpConn {
    /// Dial `addr` with a fresh UDP socket bound to `0.0.0.0:0` / `[::]:0`.
    ///
    /// ```no_run
    /// use kcp_rs::KcpConn;
    /// # fn main() {
    /// # let _fut = async {
    /// let conn = KcpConn::connect("127.0.0.1:29900").mtu(1400).build().await?;
    /// # Ok::<_, std::io::Error>(conn)
    /// # };
    /// # }
    /// ```
    pub fn connect(addr: impl ToSocketAddrs) -> KcpConnBuilder {
        match resolve_one(addr) {
            Ok(remote) => KcpConnBuilder {
                remote: Some(remote),
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: None,
                dial: DialTransport::Udp,
                adopt_conv: false,
                background_input: true,
                connect_timeout: None,
            },
            Err(e) => KcpConnBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::Udp,
                adopt_conv: false,
                background_input: true,
                connect_timeout: None,
            },
        }
    }

    /// Build on an existing [`PacketTransport`] (UDP / TcpRaw / CryptoTransport).
    ///
    /// By default the transport is treated as **unconnected** (`send_batch_to`).
    /// Call [`.connected(true)`](KcpConnBuilder::connected) when the socket was
    /// created via `UdpSocket::connect`.
    pub fn with_transport(
        transport: Arc<dyn PacketTransport>,
        remote: SocketAddr,
    ) -> KcpConnBuilder {
        KcpConnBuilder {
            remote: Some(remote),
            transport: Some(transport),
            config: KcpConfig::default(),
            connected: false,
            resolve_err: None,
            dial: DialTransport::Udp,
            adopt_conv: false,
            background_input: true,
            connect_timeout: None,
        }
    }

    /// Dial over Linux raw-TCP (tcpraw). Non-Linux returns `io::Unsupported`
    /// at build time (stub), matching binary `--tcp`.
    pub fn connect_tcp(addr: impl ToSocketAddrs) -> KcpConnBuilder {
        match resolve_one(addr) {
            Ok(remote) => KcpConnBuilder {
                remote: Some(remote),
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: None,
                dial: DialTransport::TcpRaw,
                adopt_conv: false,
                background_input: true,
                connect_timeout: None,
            },
            Err(e) => KcpConnBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::TcpRaw,
                adopt_conv: false,
                background_input: true,
                connect_timeout: None,
            },
        }
    }

    // ── KCP-specific tuning ────────────────────────────────────────────────
    // Prefixed `set_kcp_*` so the plain `set_*` names stay reserved for the
    // TcpStream-aligned surface below (no name collisions).

    /// Dynamic nodelay tweak after construction (KCP-native 4-knob form).
    pub fn set_kcp_nodelay(&self, nodelay: u32, interval: u32, resend: u32, nc: u32) {
        self.shared
            .kcp
            .lock()
            .set_nodelay(nodelay, interval, resend, nc);
    }

    /// Adjust the KCP send/receive window sizes after construction.
    pub fn set_kcp_window_size(&self, snd_wnd: u32, rcv_wnd: u32) {
        let mut kcp = self.shared.kcp.lock();
        kcp.set_snd_wnd(snd_wnd);
        kcp.set_rcv_wnd(rcv_wnd);
        let effective_snd_wnd = kcp.snd_wnd() as usize;
        self.shared
            .snd_wnd
            .store(effective_snd_wnd, Ordering::Relaxed);
        if self.shared.backpressure_relieved() {
            self.shared.write_notify.notify_one();
        }
    }

    /// Adjust the KCP MTU after construction.
    pub fn set_kcp_mtu(&self, mtu: u32) {
        self.shared.kcp.lock().set_mtu(mtu);
    }

    /// Toggle KCP stream mode (concatenate small sends into segments) after construction.
    pub fn set_kcp_stream_mode(&self, enable: bool) {
        self.shared.kcp.lock().set_stream_mode(enable);
    }

    /// Toggle KCP ACK-no-delay (send an ACK immediately on input) after construction.
    pub fn set_kcp_acknodelay(&self, enable: bool) {
        self.shared.acknodelay.store(enable, Ordering::Release);
    }

    // ── TcpStream-aligned surface ────────────────────────────────────────────

    /// TCP-style Nagle toggle. Maps to the KCP fast path
    /// (`nodelay=1, interval=10, resend=2, nc=1`) when `true`, and the normal
    /// path (`nodelay=0, interval=40, resend=2, nc=1`) when `false`. For full
    /// KCP control use [`set_kcp_nodelay`](Self::set_kcp_nodelay).
    pub fn set_nodelay(&self, nodelay: bool) {
        self.shared.nodelay.store(nodelay, Ordering::Release);
        let (n, i, r, c) = if nodelay {
            (1, 10, 2, 1)
        } else {
            (0, 40, 2, 1)
        };
        self.shared.kcp.lock().set_nodelay(n, i, r, c);
    }

    /// Last value passed to [`set_nodelay`](Self::set_nodelay) (or the builder's
    /// configured mode on construction).
    pub fn nodelay(&self) -> bool {
        self.shared.nodelay.load(Ordering::Acquire)
    }

    /// Surface the last non-transient I/O error from the background loops,
    /// clearing it. Mirrors `std::net::TcpStream::take_error`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(self.shared.last_error.lock().take())
    }

    /// Peek at already-buffered inbound bytes without consuming them.
    ///
    /// Unlike `std::net::TcpStream::peek`, `KcpConn` is inherently async, so
    /// this is **non-blocking**: it returns [`io::ErrorKind::WouldBlock`] when
    /// no data has arrived yet. Use [`readable`](Self::readable) to await data
    /// first, then `peek`.
    pub fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        if self.shared.read_closed.load(Ordering::Acquire) {
            return Ok(0);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        {
            let rb = self.shared.read_buf.lock();
            if let Some(data) = rb.front() {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                return Ok(n);
            }
        }
        // With direct KCP reads, complete data normally remains in KCP rather
        // than the spill slot. Peek the first queued segment without consuming
        // it. (A fragmented message exposes its first segment, which is still
        // sufficient for the non-consuming readiness contract.)
        let kcp = self.shared.kcp.lock();
        let Some(_) = kcp.peeksize() else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "no buffered data to peek",
            ));
        };
        let Some(seg) = kcp.peek_recv() else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "no buffered data to peek",
            ));
        };
        let n = seg.data.len().min(buf.len());
        buf[..n].copy_from_slice(&seg.data[..n]);
        Ok(n)
    }

    /// Shut down the read and/or write half, mirroring
    /// `std::net::TcpStream::shutdown`.
    ///
    /// - [`Shutdown::Write`]: stop accepting writes (`poll_write` returns
    ///   `BrokenPipe`); queued data is still flushed. The **peer is not
    ///   notified** — KCP has no wire FIN, so peer-aware half-close lives at the
    ///   SMUX/session layer.
    /// - [`Shutdown::Read`]: stop surfacing inbound data; `poll_read` returns
    ///   `Ok(0)` (EOF).
    /// - [`Shutdown::Both`]: equivalent to [`close`](Self::close).
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match how {
            Shutdown::Read => {
                self.shared.read_closed.store(true, Ordering::Release);
                self.shared.read_buf.lock().clear();
                *self.shared.read_deadline.lock() = None;
                self.shared.wake_reader();
            }
            Shutdown::Write => {
                self.shared.write_closed.store(true, Ordering::Release);
                self.shared.flush_notify.notify_one();
                self.shared.wake_writer();
            }
            Shutdown::Both => {
                self.shared.read_closed.store(true, Ordering::Release);
                self.shared.write_closed.store(true, Ordering::Release);
                self.shared.close();
            }
        }
        Ok(())
    }

    /// Set the read timeout. [`read_shared`](Self::read_shared), `poll_read`,
    /// and [`readable`](Self::readable) return [`io::ErrorKind::TimedOut`] after
    /// it elapses with no data. `None` disables.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        *self.shared.read_timeout.lock() = dur.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// Current read timeout.
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.shared.read_timeout.lock().map(Duration::from_millis))
    }

    /// Set the write timeout. [`write_all_shared`](Self::write_all_shared) and
    /// `poll_write` return [`io::ErrorKind::TimedOut`] after it elapses while
    /// blocked on a full send window. `None` disables.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        *self.shared.write_timeout.lock() = dur.map(|d| d.as_millis() as u64);
        Ok(())
    }

    /// Current write timeout.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.shared.write_timeout.lock().map(Duration::from_millis))
    }

    /// Check for a partial read-buffer segment or a complete KCP message.
    /// Locks are deliberately acquired separately: the input path only holds
    /// the KCP lock, while readers only hold the read-buffer lock while
    /// inspecting the spill slot.
    fn has_read_data(&self) -> bool {
        if !self.shared.read_buf.lock().is_empty() {
            return true;
        }
        self.shared.kcp.lock().peeksize().is_some()
    }

    /// Copy available bytes from the bounded prefetch/spill queue and directly
    /// from KCP's receive queue. Concurrent readers are serialized; after
    /// acquiring KCP we recheck the queue so prefetched data cannot be
    /// overtaken by a direct read.
    fn read_available(&self, out: &mut [u8]) -> usize {
        let _read_op = self.shared.read_op.lock();
        let mut filled = 0usize;
        let mut consumed_kcp = false;

        loop {
            // First consume prefetched messages and any partial segment left
            // by an earlier short read.
            {
                let mut rb = self.shared.read_buf.lock();
                while filled < out.len() {
                    let Some(mut data) = rb.pop_front() else {
                        break;
                    };
                    let n = data.len().min(out.len() - filled);
                    out[filled..filled + n].copy_from_slice(&data[..n]);
                    filled += n;
                    if n < data.len() {
                        let _ = data.split_to(n);
                        rb.push_front(data);
                        break;
                    }
                }
            }
            if filled >= out.len() {
                break;
            }

            let mut kcp = self.shared.kcp.lock();
            // An input task may have prefetched between the first drain and
            // this lock acquisition. KCP excludes further prefetch while this
            // check runs; retry the queue to preserve strict FIFO order.
            if !self.shared.read_buf.lock().is_empty() {
                drop(kcp);
                continue;
            }
            while filled < out.len() {
                let data = match kcp.recv_bytes() {
                    Ok(data) if !data.is_empty() => data,
                    _ => break,
                };
                consumed_kcp = true;
                let n = data.len().min(out.len() - filled);
                out[filled..filled + n].copy_from_slice(&data[..n]);
                filled += n;
                if n < data.len() {
                    // Keep KCP held until the earlier remainder is visible;
                    // input-side prefetch uses the same KCP → read_buf order.
                    self.shared.read_buf.lock().push_front(data.slice(n..));
                    break;
                }
            }
            break;
        }

        // recv_bytes() may set KCP's ASK_TELL probe when the receive window
        // opens. Let the protocol-deadline flush loop emit that WINS packet.
        if consumed_kcp {
            self.shared.flush_notify.notify_one();
        }
        filled
    }

    /// Wait until data is available to read (or the connection is closed).
    /// Mirrors `tokio::net::TcpStream::readable`.
    pub async fn readable(&self) -> io::Result<()> {
        let deadline = self
            .shared
            .read_timeout
            .lock()
            .map(|ms| kio::mono_ms().saturating_add(ms));
        loop {
            if self.shared.read_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "KcpConn read half closed",
                ));
            }
            if self.has_read_data() {
                return Ok(());
            }
            if self.shared.is_closed() || self.shared.read_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "KcpConn closed",
                ));
            }
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(kio::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                    }
                    match kio::timeout(
                        Duration::from_millis(remaining),
                        kio::race(
                            Box::pin(self.shared.read_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(kio::RaceOutcome::First(_)) | Ok(kio::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                        }
                    }
                }
                None => {
                    let _ = kio::race(
                        Box::pin(self.shared.read_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    /// Wait until the send window has room (or the connection is closed).
    /// Mirrors `tokio::net::TcpStream::writable`.
    pub async fn writable(&self) -> io::Result<()> {
        let deadline = self
            .shared
            .write_timeout
            .lock()
            .map(|ms| kio::mono_ms().saturating_add(ms));
        loop {
            if self.shared.is_closed() || self.shared.write_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "KcpConn closed"));
            }
            if self.shared.backpressure_relieved() {
                return Ok(());
            }
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(kio::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                    }
                    match kio::timeout(
                        Duration::from_millis(remaining),
                        kio::race(
                            Box::pin(self.shared.write_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(kio::RaceOutcome::First(_)) | Ok(kio::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                        }
                    }
                }
                None => {
                    let _ = kio::race(
                        Box::pin(self.shared.write_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Monotonic timestamp in milliseconds of the latest successful read or write.
    pub fn last_activity_ms(&self) -> u64 {
        self.shared.last_activity_ms.load(Ordering::Relaxed)
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.shared.remote_addr
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.transport.local_addr()
    }

    /// Configured send window (for diagnostics / backpressure).
    pub fn snd_wnd(&self) -> usize {
        self.shared.snd_wnd.load(Ordering::Relaxed)
    }

    /// Configured receive window (for diagnostics).
    pub fn rcv_wnd(&self) -> u32 {
        self.shared.kcp.lock().rcv_wnd()
    }

    /// Current KCP wait_send snapshot.
    pub fn wait_send(&self) -> usize {
        self.shared.wait_send.load(Ordering::Relaxed)
    }

    /// Whether KCP has declared the link dead (retransmission budget spent).
    ///
    /// The background flush loop keeps running after this; callers (the
    /// binaries' dead-link detection) poll it and tear down the session.
    pub fn is_dead(&self) -> bool {
        self.shared.kcp.lock().is_dead()
    }

    /// Feed one inbound datagram into the KCP input chain (decrypt → FEC → KCP
    /// input → KCP receive queue) and emit ACKs via the deferred batch flush.
    ///
    /// Used by the Acceptor + Worker sharding prototype when the connection was
    /// built with `.background_input(false)`: an external worker drives inbound
    /// on its own runtime instead of the process-wide background input loop.
    /// Synchronous — does all work on the calling thread (the worker's).
    pub fn feed_input(&self, data: Vec<u8>) -> io::Result<()> {
        self.feed_batch(vec![data])
    }

    /// Batch variant of [`feed_input`](Self::feed_input): processes a whole
    /// burst under one KCP lock and one deferred ACK flush (matching the
    /// background input loop's burst batching), reducing per-datagram
    /// lock/flush overhead when the driver collects a batch before feeding.
    pub fn feed_batch(&self, datagrams: Vec<Vec<u8>>) -> io::Result<()> {
        if self.shared.is_closed() {
            return Ok(());
        }
        let mut prefetched = Vec::new();
        let (data_ready, _) = process_inbound_batch(&self.shared, &datagrams, &mut prefetched);
        if data_ready {
            self.shared.wake_reader();
        }
        // External driver (not a background task) — notify flush loop to send
        // any packets produced by the deferred flush above.
        self.shared.flush_notify.notify_one();
        Ok(())
    }

    /// Async read borrowing `&self` — safe for **concurrent** read/write tasks
    /// (the internal state is already shared behind mutexes/atomics).
    ///
    /// Mirrors `poll_read_into` semantics without needing `Pin<&mut Self>`.
    /// Waits on an internal notify when no KCP data is available (wake is immediate
    /// on data arrival; `close()` calls `notify_waiters()` to wake on close).
    pub async fn read_shared(&self, buf: &mut [u8]) -> io::Result<usize> {
        let deadline = self
            .shared
            .read_timeout
            .lock()
            .map(|ms| kio::mono_ms().saturating_add(ms));
        loop {
            if buf.is_empty() {
                return Ok(0);
            }
            if self.shared.read_closed.load(Ordering::Acquire) {
                return Ok(0);
            }
            let filled = self.read_available(buf);
            if filled > 0 {
                return Ok(filled);
            }
            if self.shared.is_closed() || self.shared.read_closed.load(Ordering::Acquire) {
                return Ok(0);
            }
            // A configured read timeout takes precedence. With no timeout,
            // wait on the permit-storing Notify indefinitely; a periodic
            // fallback here creates needless timer-wheel work and tail jitter.
            match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_sub(kio::mono_ms());
                    if remaining == 0 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                    }
                    match kio::timeout(
                        Duration::from_millis(remaining),
                        kio::race(
                            Box::pin(self.shared.read_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        ),
                    )
                    .await
                    {
                        Ok(kio::RaceOutcome::First(_)) | Ok(kio::RaceOutcome::Second(_)) => {}
                        Err(_) => {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                        }
                    }
                }
                None => {
                    let _ = kio::race(
                        Box::pin(self.shared.read_notify.notified()),
                        self.shared.cancel_token.cancelled(),
                    )
                    .await;
                }
            }
        }
    }

    /// Async `write_all` borrowing `&self` — safe for concurrent read/write.
    ///
    /// Inline fast path (kcp.Send + kcp.flush under the KCP lock, matching
    /// kcp-go `UDPSession.Write`), then drains the resulting wire segments and
    /// sends them immediately — bypassing the background flush loop's 1–2ms
    /// wake-up that would otherwise add one hop per write (raw-KCP latency).
    /// When the send window is full it waits on the write notify.
    pub async fn write_all_shared(&self, buf: &[u8]) -> io::Result<()> {
        let mut offset = 0usize;
        let mut blocked_deadline = None;
        while offset < buf.len() {
            if self.shared.is_closed() || self.shared.write_closed.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "KcpConn closed"));
            }
            // Inline send + flush under the KCP lock; NO await while the guard
            // is held (the guard is `!Send`, so all `.await`s live outside).
            let sent = {
                let mut kcp = self.shared.kcp.lock();
                let ws = kcp.wait_send() as usize;
                if ws >= self.shared.snd_wnd.load(Ordering::Relaxed) {
                    drop(kcp);
                    0
                } else {
                    let sent = self.shared.send_to_kcp(&mut kcp, &buf[offset..]);
                    drop(kcp);
                    self.shared.write_notify.notify_one();
                    sent
                }
            };
            // Inline send: drain raw_packets and send directly, bypassing the
            // flush loop's scheduling hop.  Falls back to `notify_one()` when
            // the flush loop is currently sending (is_sending=true).  This
            // matches Go's synchronous `UDPSession.Write` send path.
            if sent > 0 {
                blocked_deadline = None;
                let _ = self.shared.try_drain_and_send().await;
                // Inline UDP send handles the current packet, but KCP still
                // needs a retransmission deadline if that packet is lost.
                // Always wake the protocol loop so an idle parked connection
                // arms maintenance even when we acquired the send token.
                self.shared.flush_notify.notify_one();
            }
            if sent == 0 {
                // Send window full — strict backpressure matching Go's
                // `UDPSession.Write` (blocks on `chWriteEvent` when the window
                // is full, no extra buffering). This unifies `write_all_shared`
                // with `do_poll_write` which also returns Pending on window
                // full (P1 #7: previously `write_all_shared` buffered up to
                // one extra window, causing different in-flight data / latency
                // / memory peaks depending on which write API the caller used).
                // Copy the timeout out first: the parking_lot guard is `!Send`
                // and must not be held across the `.await` below.
                let write_timeout_ms = *self.shared.write_timeout.lock();
                match write_timeout_ms {
                    Some(ms) => {
                        let deadline = *blocked_deadline
                            .get_or_insert_with(|| kio::mono_ms().saturating_add(ms));
                        let remaining = deadline.saturating_sub(kio::mono_ms());
                        if remaining == 0 {
                            if !self.shared.backpressure_relieved() {
                                return Err(io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "write timed out",
                                ));
                            }
                            blocked_deadline = None;
                            continue;
                        }
                        match kio::timeout(
                            Duration::from_millis(remaining.max(1)),
                            kio::race(
                                Box::pin(self.shared.write_notify.notified()),
                                self.shared.cancel_token.cancelled(),
                            ),
                        )
                        .await
                        {
                            Ok(kio::RaceOutcome::First(_)) | Ok(kio::RaceOutcome::Second(_)) => {}
                            Err(_) => {
                                if !self.shared.backpressure_relieved() {
                                    return Err(io::Error::new(
                                        io::ErrorKind::TimedOut,
                                        "write timed out",
                                    ));
                                }
                                blocked_deadline = None;
                            }
                        }
                    }
                    None => {
                        let _ = kio::race(
                            Box::pin(self.shared.write_notify.notified()),
                            self.shared.cancel_token.cancelled(),
                        )
                        .await;
                    }
                }
                continue;
            }
            offset += sent;
        }
        Ok(())
    }
}

impl Drop for KcpConn {
    fn drop(&mut self) {
        // Only the owner closes the connection.  Clones share the same
        // `Arc<KcpConnShared>` but must not close it when dropped.
        if self.owns_connection {
            self.shared.close();
        }
    }
}

impl Clone for KcpConn {
    /// Clone shares the same background tasks, KCP state, and transport.
    /// The clone does NOT own the connection — dropping it will NOT close
    /// the connection.  Only the original `KcpConn`'s `Drop` closes it.
    fn clone(&self) -> Self {
        KcpConn {
            shared: self.shared.clone(),
            _handles: Vec::new(),
            owns_connection: false,
        }
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

macro_rules! kcp_config_setters {
    () => {
        pub fn mtu(mut self, value: u32) -> Self {
            self.config.mtu = value;
            self
        }

        pub fn sndwnd(mut self, value: u32) -> Self {
            self.config.sndwnd = value;
            self
        }

        pub fn rcvwnd(mut self, value: u32) -> Self {
            self.config.rcvwnd = value;
            self
        }

        pub fn mode(mut self, value: KcpMode) -> Self {
            self.config.mode = value;
            self
        }

        pub fn stream(mut self, value: bool) -> Self {
            self.config.stream = value;
            self
        }

        pub fn acknodelay(mut self, value: bool) -> Self {
            self.config.acknodelay = value;
            self
        }

        pub fn conv(mut self, value: u32) -> Self {
            self.config.conv = value;
            self
        }

        pub fn token(mut self, value: u32) -> Self {
            self.config.token = value;
            self
        }

        pub fn nodelay(mut self, nodelay: u32, interval: u32, resend: u32, nc: u32) -> Self {
            self.config.mode = KcpMode::Manual;
            self.config.nodelay = nodelay;
            self.config.interval = interval;
            self.config.resend = resend;
            self.config.nc = nc;
            self
        }

        /// Enable Reed-Solomon FEC (`datashard` / `parityshard`, both must be > 0).
        pub fn fec(mut self, datashard: u32, parityshard: u32) -> Self {
            self.config.datashard = datashard;
            self.config.parityshard = parityshard;
            self
        }

        pub fn config(mut self, config: KcpConfig) -> Self {
            self.config = config;
            self
        }
    };
}

// Shared with the listener builders (KcpListenerBuilder / KcpTcpListenerBuilder).
pub(crate) use kcp_config_setters;

/// Builder for [`KcpConn`]. Call [`.build().await`](Self::build) to construct.
pub struct KcpConnBuilder {
    remote: Option<SocketAddr>,
    transport: Option<Arc<dyn PacketTransport>>,
    config: KcpConfig,
    connected: bool,
    resolve_err: Option<io::Error>,
    dial: DialTransport,
    adopt_conv: bool,
    background_input: bool,
    connect_timeout: Option<Duration>,
}

impl KcpConnBuilder {
    kcp_config_setters!();

    /// Adopt `conv` from the first valid inbound KCP segment.
    ///
    /// Server listeners use this because Go kcptun clients choose the
    /// conversation ID; dialed client connections keep their configured ID.
    pub fn adopt_conv(mut self, enabled: bool) -> Self {
        self.adopt_conv = enabled;
        self
    }

    /// Whether the transport is already `connect()`ed (use `send` / `send_batch`).
    ///
    /// Default: `true` for [`KcpConn::connect`], `false` for [`KcpConn::with_transport`].
    pub fn connected(mut self, v: bool) -> Self {
        self.connected = v;
        self
    }

    /// Spawn the background input-loop task (default `true`).
    ///
    /// Set to `false` when an external driver feeds inbound via
    /// [`KcpConn::feed_input`] (the Acceptor + Worker sharding prototype), so
    /// the connection's tasks stay on the driver's runtime instead of being
    /// scheduled onto the process-wide executor.
    pub fn background_input(mut self, enabled: bool) -> Self {
        self.background_input = enabled;
        self
    }

    /// Require the first conv-valid inbound packet (peer probe `WINS` / ACK)
    /// within `timeout`, failing [`build`](Self::build) with `TimedOut`
    /// otherwise. KCP has no handshake, so this is a **reachability + conv
    /// match** check, not a connection-established handshake: the dialing side
    /// forces a `WASK` probe immediately and waits for any valid response.
    ///
    /// Requires the default background input loop (`background_input(true)`).
    /// A dead peer always costs the full timeout (UDP has no RST-style fast
    /// failure). On timeout the connection is closed and dropped.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Deprecated compatibility setter. Writes no longer use an intermediate
    /// buffer, so this value is intentionally ignored.
    pub fn buffer_size(self, _bytes: usize) -> Self {
        self
    }

    /// Construct the connection and start background input/flush loops.
    pub async fn build(self) -> io::Result<KcpConn> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let remote = self.remote.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpConn: remote address required",
            )
        })?;

        let (transport, connected) = if let Some(t) = self.transport {
            (t, self.connected)
        } else {
            match self.dial {
                DialTransport::Udp => {
                    let bind = if remote.is_ipv4() {
                        SocketAddr::from(([0, 0, 0, 0], 0))
                    } else {
                        SocketAddr::from(([0u16; 8], 0))
                    };
                    let udp = kio::UdpSocket::connect(bind, remote)?;
                    let sock: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::Udp(udp));
                    (sock, true)
                }
                DialTransport::TcpRaw => {
                    let conn = kio::tcpraw_dial(&remote)?;
                    let sock: Arc<dyn PacketTransport> =
                        Arc::new(kio::DatagramSocket::TcpRaw(conn));
                    (sock, true)
                }
            }
        };

        let config = self.config;
        let raw_packets = Arc::new(Mutex::new(RawPacketQueue::default()));
        let raw_packets_cb = raw_packets.clone();

        let mut kcp = KCP::new(config.conv, config.token, move |data: Bytes| {
            crate::snmp::add(&crate::snmp::DEFAULT_SNMP.out_pkts, 1);
            raw_packets_cb.lock().push(data);
        });
        kcp.apply(&config);
        let effective_snd_wnd = kcp.snd_wnd() as usize;

        // FEC (header_offset=0): crypto would wrap the whole FEC frame later.
        let (fec_encoder, fec_decoder) = if config.datashard > 0 && config.parityshard > 0 {
            let d = config.datashard as usize;
            let p = config.parityshard as usize;
            (
                FecEncoder::new(d, p, 0).map(Mutex::new),
                FecDecoder::new(d, p).map(Mutex::new),
            )
        } else {
            (None, None)
        };

        let shared = Arc::new(KcpConnShared {
            transport,
            kcp: Arc::new(Mutex::new(kcp)),
            read_buf: Mutex::new(ReadBuffer::default()),
            read_op: Mutex::new(()),
            raw_packets,
            flush_notify: Arc::new(kio::Notify::new()),
            write_notify: Arc::new(kio::Notify::new()),
            read_notify: Arc::new(kio::Notify::new()),
            read_waker: Mutex::new(None),
            write_waker: Mutex::new(None),
            wait_send: Arc::new(AtomicUsize::new(0)),
            snd_wnd: AtomicUsize::new(effective_snd_wnd),
            acknodelay: AtomicBool::new(config.acknodelay),
            remote_addr: remote,
            connected,
            closed: Arc::new(AtomicBool::new(false)),
            cancel_token: CancellationToken::new(),
            adopt_conv: AtomicBool::new(self.adopt_conv),
            background_input: self.background_input,
            last_activity_ms: AtomicU64::new(kio::mono_ms()),
            is_sending: AtomicBool::new(false),
            fec_encoder,
            fec_decoder,
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            read_deadline: Mutex::new(None),
            write_deadline: Mutex::new(None),
            last_error: Mutex::new(None),
            nodelay: AtomicBool::new(
                config
                    .mode
                    .nodelay_params()
                    .map(|(n, ..)| n != 0)
                    .unwrap_or(config.nodelay != 0),
            ),
            write_closed: AtomicBool::new(false),
            read_closed: AtomicBool::new(false),
            first_inbound: AtomicBool::new(false),
        });

        let mut handles = Vec::with_capacity(2);
        if shared.background_input {
            handles.push(spawn_input_loop(shared.clone()));
        }
        handles.push(spawn_flush_loop(shared.clone()));

        let conn = KcpConn {
            shared: shared.clone(),
            _handles: handles,
            owns_connection: true,
        };

        // Optional connect-timeout: force a `WASK` probe and wait for the first
        // conv-valid inbound (peer `WINS` / ACK) within the deadline. KCP has no
        // handshake, so this proves reachability + conv match, not connection
        // establishment.
        if let Some(t) = self.connect_timeout {
            if !shared.background_input {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connect_timeout requires the background input loop (background_input(true))",
                ));
            }
            shared.kcp.lock().request_probe();
            shared.flush_notify.notify_one();
            match kio::timeout(t, async {
                loop {
                    if shared.first_inbound.load(Ordering::Acquire) {
                        return;
                    }
                    if shared.is_closed() {
                        return;
                    }
                    // Safety-net timeout recovers lost-wake races.
                    let _ = kio::timeout(
                        Duration::from_millis(WAIT_FALLBACK_MS),
                        shared.read_notify.notified(),
                    )
                    .await;
                }
            })
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    shared.close();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "KCP connect timeout: no response from {} within {}ms",
                            remote,
                            t.as_millis()
                        ),
                    ));
                }
            }
            if !shared.first_inbound.load(Ordering::Acquire) {
                shared.close();
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "KCP connection closed during connect",
                ));
            }
        }

        Ok(conn)
    }
}

/// `KcpConn::connect(addr).await` — awaitable without an explicit `.build()`.
impl std::future::IntoFuture for KcpConnBuilder {
    type Output = io::Result<KcpConn>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

pub(crate) fn resolve_one(addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve address"))
}

// ─── Background loops ─────────────────────────────────────────────────────────

fn spawn_input_loop(shared: Arc<KcpConnShared>) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        // Pre-allocate burst capacity to avoid dynamic growth spikes.
        // MAX_INPUT_BATCH slots × MAX_DATAGRAM bytes = ~96KB per connection,
        // allocated once at task start and recycled across all cycles.
        let mut burst: Vec<Vec<u8>> = Vec::with_capacity(MAX_INPUT_BATCH);
        burst.push(vec![0u8; MAX_DATAGRAM]);
        // Reused landing vector for the small bounded prefetch published by
        // `process_inbound_batch`; retaining its capacity avoids a per-burst
        // allocation without retaining payload bytes.
        let mut prefetched: Vec<Bytes> = Vec::with_capacity(READ_PREFETCH_MAX_MESSAGES);
        loop {
            if shared.is_closed() {
                break;
            }
            burst[0].resize(MAX_DATAGRAM, 0);
            // `close()` cancels the socket recv directly via the cancellation
            // token, so a closed-but-silent peer's task exits immediately
            // instead of waiting out a 100ms poll tick — no ~10 Hz timer churn
            // per idle connection. Active links complete `recv` normally.
            let n = match kio::race(
                Box::pin(shared.transport.recv_vec(&mut burst[0])),
                shared.cancel_token.cancelled(),
            )
            .await
            {
                kio::RaceOutcome::First(Ok(n)) if n > 0 => n,
                kio::RaceOutcome::First(Ok(_)) => continue,
                kio::RaceOutcome::First(Err(_)) if shared.is_closed() => break,
                kio::RaceOutcome::First(Err(e)) => {
                    *shared.last_error.lock() = Some(e);
                    kio::sleep_ms(10).await;
                    continue;
                }
                kio::RaceOutcome::Second(_) => break, // close() cancelled the recv
            };
            shared
                .last_activity_ms
                .store(kio::mono_ms(), Ordering::Relaxed);

            // Collect the full recv burst first, then process all datagrams
            // in one batch: FEC decode outside the KCP lock, one KCP lock for
            // input + deferred flush, and one reader wake. Leaving payload in
            // KCP preserves its receive-window backpressure; Reed-Solomon
            // decode remains outside the KCP state-machine lock.
            burst[0].truncate(n);
            let mut burst_len = 1;
            if shared.transport.supports_recv_batch() {
                // Batch drain: fill pre-sized slots via `try_recv_batch` (the
                // listener's `PeerTransport` pops the whole queue under one
                // lock; a direct-UDP client gets recvmmsg). Bounded by
                // MAX_INPUT_BATCH per cycle so a high-rate peer cannot starve
                // the worker (v3 §5.4). Slots are recycled across cycles.
                loop {
                    while burst.len() < burst_len + INPUT_BATCH_GROW {
                        burst.push(vec![0u8; MAX_DATAGRAM]);
                    }
                    let pool_end = (burst_len + INPUT_BATCH_GROW).min(MAX_INPUT_BATCH);
                    if pool_end <= burst_len {
                        break; // per-cycle budget reached
                    }
                    for s in &mut burst[burst_len..pool_end] {
                        s.resize(MAX_DATAGRAM, 0);
                    }
                    match shared
                        .transport
                        .try_recv_batch(&mut burst[burst_len..pool_end])
                    {
                        Ok(k) if k > 0 => burst_len += k,
                        _ => break, // WouldBlock / empty — peer drained
                    }
                    if burst_len >= MAX_INPUT_BATCH {
                        break; // defer the rest to the next cycle
                    }
                }
            } else {
                // Fallback: sequential single-packet drain.
                loop {
                    if burst_len == burst.len() {
                        burst.push(vec![0u8; MAX_DATAGRAM]);
                    } else {
                        burst[burst_len].resize(MAX_DATAGRAM, 0);
                    }
                    match shared.transport.try_recv_vec(&mut burst[burst_len]) {
                        Ok(m) if m > 0 => {
                            burst[burst_len].truncate(m);
                            burst_len += 1;
                        }
                        _ => break,
                    }
                }
            }
            // KCP input + the burst's single deferred flush share ONE mutex
            // acquisition (see `process_inbound_batch`). Produced packets stay
            // in `raw_packets`; the flush loop is the ONLY drainer + sender, so
            // wire order = flush order (single-owner — the old inline ACK send
            // here raced the flush loop and interleaved batches on the wire).
            let (data_ready, protocol_pending) =
                process_inbound_batch(&shared, &burst[..burst_len], &mut prefetched);
            if data_ready {
                shared.wake_reader();
            }
            // Runtime-conditional send strategy (P99 optimization):
            //
            // smol (2 threads): inline-send eliminates the cross-thread
            // notify→wake→drain→send hop. With only 2 executor threads,
            // waking the flush loop means preempting this input loop, so recv
            // stalls and P99 degrades (measured ~21ms → ~8.6ms P99 median).
            // The `is_sending` CAS ensures single-owner wire order: if the
            // flush loop is already sending, fall back to notify.
            //
            // tokio (N threads): keep the notify path — inline-send loses the
            // input/flush cross-core parallelism and worsens P99 by ~66%.
            // `runtime_kind()` is `const fn`, so the smol branch is dead-code
            // eliminated in tokio builds (and vice-versa) — zero runtime cost.
            if kio::runtime_kind() == kio::RuntimeKind::Smol {
                let sent_inline = shared.try_drain_and_send().await;
                if !sent_inline || protocol_pending {
                    // If another sender owns the raw queue, or KCP still has a
                    // delayed ACK/probe/retransmission deadline, arm the
                    // maintenance loop. A completed inline ACK-only burst does
                    // not need a redundant task hop.
                    shared.flush_notify.notify_one();
                }
            } else {
                shared.flush_notify.notify_one();
            }
        }
    })
}

/// Feed a burst of inbound datagrams into KCP and run the burst's single
/// deferred flush.
///
/// Three-phase design (P0 #3, P1 #4, P1 #5):
///
/// 1. **FEC decode OUTSIDE the KCP lock** — Reed-Solomon matrix operations are
///    CPU-heavy and used to run while holding the KCP mutex, blocking writes,
///    flushes, and ACK processing. Now the FEC decoder lock is acquired and
///    released before the KCP lock is touched.
///
/// 2. **One KCP lock for the whole burst** — all `input_no_flush` calls and the
///    deferred ACK flush happen under a single mutex acquisition. At most a
///    small byte-bounded batch is prefetched for read/input pipelining; excess
///    data stays in KCP so receive-window backpressure remains bounded.
///
/// Produced wire packets (ACKs / data / probes) stay in `raw_packets`; the
/// caller chooses the send strategy (inline vs. flush-loop notify).
///
/// Returns `(data_ready, protocol_pending)` for reader and maintenance wakes.
fn process_inbound_batch(
    shared: &KcpConnShared,
    datagrams: &[Vec<u8>],
    prefetched: &mut Vec<Bytes>,
) -> (bool, bool) {
    // ── Phase 1: FEC decode all datagrams OUTSIDE the KCP lock ──
    // For non-FEC mode, datagrams are fed directly in Phase 2 (no clone).
    // For FEC mode, original data shards stay borrowed. Only reconstructed
    // shards need owned storage because the decoder's result is temporary.
    let has_fec = shared.fec_decoder.is_some();
    let mut kcp_slices: Vec<Cow<'_, [u8]>> = Vec::new();

    if has_fec {
        // Decode the complete burst while holding the decoder mutex once.
        // Reed-Solomon state is per-peer, and repeatedly locking it for every
        // datagram amplified contention under recvmmsg bursts.
        let dec = shared.fec_decoder.as_ref().unwrap();
        let mut decoder = dec.lock();
        for input in datagrams {
            crate::snmp::add(&crate::snmp::DEFAULT_SNMP.in_pkts, 1);
            if input.len() >= 6 {
                let fec_flag = u16::from_le_bytes([input[4], input[5]]);
                let recovered = decoder.decode(input);
                match fec_flag {
                    FEC_TYPE_DATA => {
                        if input.len() > FEC_HDR {
                            kcp_slices.push(Cow::Borrowed(&input[FEC_HDR..]));
                        }
                        for r in &recovered {
                            if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                                kcp_slices.push(Cow::Owned(kcp_slice.to_vec()));
                            }
                        }
                    }
                    FEC_TYPE_PARITY => {
                        for r in &recovered {
                            if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                                kcp_slices.push(Cow::Owned(kcp_slice.to_vec()));
                            }
                        }
                    }
                    _ => {
                        if input.len() >= 24 {
                            kcp_slices.push(Cow::Borrowed(input));
                        }
                    }
                }
            } else if input.len() >= 24 {
                kcp_slices.push(Cow::Borrowed(input));
            }
        }
    } else {
        // Non-FEC: count packets, feed directly in Phase 2 (no clone).
        for _ in datagrams {
            crate::snmp::add(&crate::snmp::DEFAULT_SNMP.in_pkts, 1);
        }
    }

    // ── Phase 2: KCP input + deferred ACK flush, one lock ──
    let mut had_input = false;
    prefetched.clear();
    let (ws, data_ready, protocol_pending) = {
        let mut kcp = shared.kcp.lock();
        if has_fec {
            for slice in &kcp_slices {
                if input_with_optional_conv(&mut kcp, shared, slice.as_ref()) {
                    had_input = true;
                }
            }
        } else {
            for input in datagrams {
                if input_with_optional_conv(&mut kcp, shared, input) {
                    had_input = true;
                }
            }
        }
        // `input_no_flush` records whether ACKs/data need a flush. This keeps
        // the default ack-no-delay=true behavior immediate while avoiding a
        // sticky unconditional flush for every inbound burst.
        let current = kcp.current_ms() as u32;
        kcp.flush_if_pending(current);

        // Snapshot only after taking KCP. A direct reader cannot add a
        // short-read remainder while KCP is held; concurrent readers may only
        // decrease these values, making the budget conservative but exact.
        let (mut prefetched_bytes, mut prefetched_messages) = {
            let read_buf = shared.read_buf.lock();
            (read_buf.bytes(), read_buf.len())
        };
        while let Some(size) = kcp.peeksize() {
            let remaining = READ_PREFETCH_MAX_BYTES.saturating_sub(prefetched_bytes);
            if size == 0 || size > remaining || prefetched_messages >= READ_PREFETCH_MAX_MESSAGES {
                break;
            }
            let Ok(data) = kcp.recv_bytes() else {
                break;
            };
            prefetched_bytes = prefetched_bytes.saturating_add(data.len());
            prefetched_messages += 1;
            prefetched.push(data);
        }
        // Publish the entire burst under one read-buffer lock while KCP is
        // still held. This preserves FIFO against direct KCP readers and
        // avoids one mutex round-trip per small message.
        let queued_ready = {
            let mut read_buf = shared.read_buf.lock();
            read_buf.extend(prefetched.drain(..));
            !read_buf.is_empty()
        };
        (
            kcp.wait_send() as usize,
            queued_ready || kcp.peeksize().is_some(),
            kcp.needs_update(),
        )
    };

    // First conv-valid inbound from the peer (probe WINS / ACK / data) drives
    // the connect-timeout first-packet wait. Notify only on that one-shot
    // transition; data-ready bursts use the normal reader wake below.
    if had_input && !shared.first_inbound.swap(true, Ordering::AcqRel) {
        shared.read_notify.notify_one();
    }

    // Publish the post-flush send window: the deferred flush is what removes
    // ACKed segments from `snd_buf`, so `wait_send` is only accurate here.
    shared.wait_send.store(ws, Ordering::Relaxed);
    if ws < shared.snd_wnd.load(Ordering::Relaxed) {
        // Directly wake any blocked writer — eliminates the need for a
        // spawned backpressure task (see arm_backpressure_wake).
        shared.wake_writer();
    }

    (
        data_ready && !shared.read_closed.load(Ordering::Acquire),
        protocol_pending,
    )
}

/// Feed one decrypted KCP packet, committing server-side conv adoption only
/// after the packet passes KCP validation. Invalid traffic must not pin a
/// listener session to an attacker-controlled conversation ID.
fn input_with_optional_conv(kcp: &mut KCP, shared: &KcpConnShared, input: &[u8]) -> bool {
    if input.len() < 24 {
        return false;
    }
    if !shared.adopt_conv.load(Ordering::Acquire) {
        return kcp
            .input_no_flush(input, shared.acknodelay.load(Ordering::Acquire))
            .is_ok();
    }

    let configured = kcp.conv();
    let candidate = u32::from_le_bytes(input[..4].try_into().unwrap());
    kcp.set_conv(candidate);
    if kcp
        .input_no_flush(input, shared.acknodelay.load(Ordering::Acquire))
        .is_ok()
    {
        shared.adopt_conv.store(false, Ordering::Release);
        true
    } else {
        kcp.set_conv(configured);
        false
    }
}

fn spawn_flush_loop(shared: Arc<KcpConnShared>) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        // Absolute deadline-based scheduling (P0 #1+#2): replaces the old
        // fixed 2ms timer task + static `next_update` counter that never
        // decreased. The old design had two bugs:
        //
        // 1. `next_update` was a static "ms until next event" value that never
        //    decreased between KCP flushes — the loop's `if next_update > 1`
        //    guard caused it to skip the KCP state machine indefinitely until
        //    a write or ACK happened to wake it. On a silent link with
        //    un-ACKed data, RTO expiry was effectively starved.
        //
        // 2. The 2ms timer task fired every 2ms per connection, even when idle
        //    — 10K idle connections → ~5M wakeups/sec of pure timer-wheel churn.
        //
        // Now: sleep until the next KCP event deadline, woken early by Notify
        // when data/ACK/close arrives. Under load, the notify fires before the
        // timer; when entirely idle there is no timer at all.
        // An entirely idle connection has no retransmission, ACK, or probe
        // deadline to service. Park on Notify until the first activity instead
        // of creating one timer-wheel wake per KCP interval per connection.
        // Once activity arrives we retain the configured KCP interval before
        // the first maintenance flush, preserving delayed-ACK timing.
        // Run one initial protocol tick. Besides initializing KCP scheduling,
        // this guarantees a probe requested immediately after task spawn is
        // observed even on runtimes where a pre-listener notification is not
        // retained. The connection parks after that tick if it is still idle.
        let mut next_deadline: Option<Instant> = Some(Instant::now());
        let mut idle_candidate = false;
        let mut dead_checks: u32 = 50;
        loop {
            if shared.is_closed() {
                break;
            }

            // Wait for activity when idle, otherwise race activity against the
            // next retransmission / protocol-maintenance deadline.
            let was_notified = if let Some(deadline) = next_deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                kio::timeout(remaining, shared.flush_notify.notified())
                    .await
                    .is_ok()
            } else {
                shared.flush_notify.notified().await;
                true
            };
            if was_notified && idle_candidate {
                // Activity arrived while the connection was only waiting out
                // the one-second idle grace. That grace deadline is not a KCP
                // retransmission deadline: discard it so a sparse write arms
                // maintenance from the current protocol interval instead of
                // inheriting up to ~1s of stale delay before its first RTO.
                next_deadline = None;
            }
            if was_notified {
                idle_candidate = false;
            }

            if shared.is_closed() {
                break;
            }

            dead_checks = dead_checks.saturating_sub(1);
            if dead_checks == 0 {
                dead_checks = 50;
                // Dead-link is NOT auto-closed here: `is_dead()` can false-
                // positive under burst load (a segment's retransmission budget
                // exhausts transiently), and auto-closing kills healthy
                // connections. The binaries poll `is_dead()` / SMUX keepalive
                // themselves and tear down with their own policy.
            }

            // ── Fast send: drain + send write-path packets BEFORE touching the
            // KCP mutex ──
            //
            // Use `is_sending` token to avoid concurrent send with inline writers.
            // If the token is held (writer is sending), skip — the writer already
            // drained and sent.  We'll pick up any KCP state-machine packets in
            // the second drain below.
            let fast_acquired = shared
                .is_sending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            if fast_acquired {
                let fast_packets = shared.drain_raw_packets();
                if !fast_packets.is_empty() {
                    if let Err(e) = shared.send_packets_with_fec(&fast_packets).await {
                        *shared.last_error.lock() = Some(e);
                        crate::snmp::add(&crate::snmp::DEFAULT_SNMP.write_flush_sends, 0);
                    }
                    crate::snmp::add(&crate::snmp::DEFAULT_SNMP.write_flush_sends, 1);
                }
                shared.recycle_raw_packets(fast_packets);
                shared.finish_sending();
            }

            // ── KCP state-machine phase ──
            // Writes flush data inline; this protocol-deadline path owns
            // maintenance and may emit delayed ACKs (ack-no-delay=false).
            let ws = {
                let now = Instant::now();
                match next_deadline {
                    Some(deadline) if now < deadline => continue,
                    None => {
                        // The write/input path already emitted any immediate
                        // data or ACK batch before notifying us. Arm the first
                        // maintenance deadline using the configured protocol
                        // interval; probes and delayed ACKs are serviced then.
                        let delay_ms = shared.kcp.lock().interval() as u64;
                        next_deadline = Some(
                            now + Duration::from_millis(delay_ms.clamp(1, MAX_IDLE_UPDATE_MS)),
                        );
                        continue;
                    }
                    Some(_) => {}
                }
                let mut kcp = shared.kcp.lock();
                let current = kcp.current_ms() as u32;
                let delay_ms = kcp.flush_with_current(current, true) as u64;
                let ws = kcp.wait_send() as usize;
                next_deadline = if ws > 0 {
                    idle_candidate = false;
                    Some(now + Duration::from_millis(delay_ms.clamp(1, ACTIVE_UPDATE_MAX_MS)))
                } else if idle_candidate {
                    idle_candidate = false;
                    None
                } else {
                    idle_candidate = true;
                    Some(now + Duration::from_millis(IDLE_PARK_GRACE_MS))
                };
                ws
            };

            shared.wait_send.store(ws, Ordering::Relaxed);
            if ws < shared.snd_wnd.load(Ordering::Relaxed) {
                shared.write_notify.notify_one();
            }

            // ── Second drain: send packets produced by protocol flush ──
            let second_acquired = shared
                .is_sending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            if second_acquired {
                let packets = shared.drain_raw_packets();
                if !packets.is_empty() {
                    if let Err(e) = shared.send_packets_with_fec(&packets).await {
                        *shared.last_error.lock() = Some(e);
                        crate::snmp::add(&crate::snmp::DEFAULT_SNMP.write_flush_sends, 0);
                    }
                    crate::snmp::add(&crate::snmp::DEFAULT_SNMP.write_flush_sends, 1);
                }
                shared.recycle_raw_packets(packets);
                shared.finish_sending();
            } else {
                // Writer is sending — ensure we wake to retry sending these
                // packets on the next iteration.
                shared.flush_notify.notify_one();
            }
        }
    })
}

// ─── AsyncRead / AsyncWrite ───────────────────────────────────────────────────

impl KcpConn {
    fn poll_read_into(&self, cx: &mut Context<'_>, out: &mut [u8]) -> Poll<io::Result<usize>> {
        if out.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.shared.read_closed.load(Ordering::Acquire) {
            return Poll::Ready(Ok(0));
        }
        let waiter_changed = {
            let mut waiter = self.shared.read_waker.lock();
            let changed = waiter.as_ref().is_none_or(|old| !old.will_wake(cx.waker()));
            *waiter = Some(cx.waker().clone());
            changed
        };
        if waiter_changed {
            *self.shared.read_deadline.lock() = None;
        }
        // A read timeout armed by a previous `Pending` poll: fail once the
        // deadline passes (the timed wake re-polls the task). Copy the value
        // out so the `!Send` guard isn't held across the re-lock below.
        let deadline = *self.shared.read_deadline.lock();
        if let Some(dl) = deadline {
            if kio::mono_ms() >= dl {
                *self.shared.read_deadline.lock() = None;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "read timed out",
                )));
            }
        }
        let filled = self.read_available(out);
        if filled == 0
            && (self.shared.is_closed() || self.shared.read_closed.load(Ordering::Acquire))
        {
            return Poll::Ready(Ok(0));
        }
        if filled > 0 {
            *self.shared.read_deadline.lock() = None;
            return Poll::Ready(Ok(filled));
        }
        // Arm a one-shot read-timeout wake when a timeout is configured. Only
        // spawned when the caller opted in (`read_timeout` set), so the
        // no-timeout hot path keeps zero extra task spawns.
        let rt_ms = *self.shared.read_timeout.lock();
        let mut rd = self.shared.read_deadline.lock();
        match rt_ms {
            Some(ms) => {
                if rd.is_none() {
                    let dl = kio::mono_ms() + ms;
                    *rd = Some(dl);
                    let shared = self.shared.clone();
                    drop(rd);
                    kio::spawn_task(async move {
                        let now = kio::mono_ms();
                        kio::sleep_ms(dl.saturating_sub(now).max(1)).await;
                        // Clone the waker inside the lock, wake outside. The
                        // slot keeps its waker so the re-polling reader's
                        // `will_wake` recognizes itself and preserves the read
                        // deadline (a `take()` would clear it and re-arm this
                        // timer forever — see kcpconn_read_timeout).
                        let waker = shared.read_waker.lock().clone();
                        if let Some(w) = waker {
                            w.wake_by_ref();
                        }
                    });
                }
            }
            None => {
                *rd = None;
            }
        }
        Poll::Pending
    }

    fn arm_backpressure_wake(&self, cx: &mut Context<'_>) {
        // Store the waker so the flush loop can wake us directly when space
        // becomes available. No spawned task needed for the common case —
        // the flush loop calls `wake_writer()` when `snd_wnd` opens up after
        // ACKs.
        //
        // This eliminates per-backpressure-event task allocation (~200-500
        // bytes + timer-wheel entry) and the 1ms timer quantization that
        // added to P999 tail latency under sustained backpressure.
        //
        // Only spawn a lightweight timeout task if a write timeout is
        // configured — without it, the writer would block indefinitely if
        // backpressure persists beyond the timeout.
        {
            let mut waiter = self.shared.write_waker.lock();
            *waiter = Some(cx.waker().clone());
        }
        // Check for race: backpressure might have been relieved between the
        // check in `do_poll_write` and now. If so, wake immediately.
        if self.shared.backpressure_relieved() {
            cx.waker().wake_by_ref();
            return;
        }
        // Spawn a one-shot timeout task if configured. This is rare (only
        // when user set write_timeout), so the allocation cost is bounded.
        let deadline = *self.shared.write_deadline.lock();
        if let Some(dl) = deadline {
            let shared = self.shared.clone();
            kio::spawn_task(async move {
                let now = kio::mono_ms();
                let wait_ms = dl.saturating_sub(now);
                if wait_ms > 0 {
                    kio::sleep_ms(wait_ms).await;
                }
                // Wake the writer — `do_poll_write` will check the deadline
                // and return TimedOut if expired.
                shared.wake_writer();
            });
        }
    }

    fn do_poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.shared.is_closed() || self.shared.write_closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "KcpConn closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let waiter_changed = {
            let mut waiter = self.shared.write_waker.lock();
            let changed = waiter.as_ref().is_none_or(|old| !old.will_wake(cx.waker()));
            *waiter = Some(cx.waker().clone());
            changed
        };
        if waiter_changed {
            *self.shared.write_deadline.lock() = None;
        }
        // A write timeout armed by a previous `Pending`: fail once the deadline
        // passes (the timed wake re-polls the task). Copy the value out so the
        // `!Send` guard isn't held across the re-lock below.
        let write_deadline = *self.shared.write_deadline.lock();
        if let Some(dl) = write_deadline {
            if kio::mono_ms() >= dl {
                *self.shared.write_deadline.lock() = None;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "write timed out",
                )));
            }
        }

        // Inline fast path — mirrors kcp-go UDPSession.Write (kcp.Send +
        // kcp.flush under the KCP lock). When the send window is full, return
        // Pending and let the shared backpressure wake resume the writer.
        let sent = {
            let mut kcp = self.shared.kcp.lock();
            let ws = kcp.wait_send() as usize;
            if ws >= self.shared.snd_wnd.load(Ordering::Relaxed) {
                drop(kcp);
                // Arm the connection-wide backpressure wake, and record the
                // write deadline so a later poll fails with `TimedOut`.
                match *self.shared.write_timeout.lock() {
                    Some(ms) => {
                        let mut deadline = self.shared.write_deadline.lock();
                        if deadline.is_none() {
                            *deadline = Some(kio::mono_ms().saturating_add(ms));
                        }
                    }
                    None => *self.shared.write_deadline.lock() = None,
                }
                self.arm_backpressure_wake(cx);
                return Poll::Pending;
            }
            let sent = self.shared.send_to_kcp(&mut kcp, buf);
            drop(kcp);
            self.shared.write_notify.notify_one();
            sent
        };

        // Single-drainer send: produced segments stay in `raw_packets`; the
        // flush loop is the ONLY drainer+sender. Inline `try_send_batch` here
        // raced the flush loop's deferred sends — on a FIFO link (loopback)
        // out-of-order arrival means the sender interleaved batches, driving
        // receiver `rcv_nxt` gaps → spurious fastack → retransmit storm
        // (measured gap≈10K/2s, gmax≈511 at 256KB@RPS=500, no loss — in≈out).
        if sent > 0 {
            *self.shared.write_deadline.lock() = None;
            self.shared
                .last_activity_ms
                .store(kio::mono_ms(), Ordering::Relaxed);
            self.shared.flush_notify.notify_one();
        }

        // Return partial write when not all data was sent.  write_all will
        // call poll_write again, re-acquiring the KCP mutex for the next
        // chunk.  This matches Go's Write behavior: block when the window is
        // full rather than buffering a second in-flight window, increasing
        // latency at high RPS.
        // 3. Go's UDPSession.Write blocks immediately on chWriteEvent when
        //    the window is full, providing tighter backpressure.
        Poll::Ready(Ok(sent))
    }

    #[inline]
    fn flush_notify_hint(&self) {
        self.shared.flush_notify.notify_one();
    }
}

#[cfg(feature = "async-tokio")]
impl kio::AsyncRead for KcpConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut kio::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let space = buf.initialize_unfilled();
        match this.poll_read_into(cx, space) {
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

#[cfg(feature = "async-tokio")]
impl kio::AsyncWrite for KcpConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.do_poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flush_notify_hint();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // tokio `AsyncWrite::poll_shutdown` = write-half close (no more writes,
        // pending data still flushed). Full close stays explicit via `close()`.
        self.shared.write_closed.store(true, Ordering::Release);
        self.shared.flush_notify.notify_one();
        self.shared.wake_writer();
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "async-smol")]
impl kio::AsyncRead for KcpConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_read_into(cx, buf)
    }
}

#[cfg(feature = "async-smol")]
impl kio::AsyncWrite for KcpConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.do_poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flush_notify_hint();
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // smol `AsyncWrite::poll_close` = write-half close (see poll_shutdown).
        self.shared.write_closed.store(true, Ordering::Release);
        self.shared.flush_notify.notify_one();
        self.shared.wake_writer();
        Poll::Ready(Ok(()))
    }
}

// ─── Split halves (tokio-style) ───────────────────────────────────────────────

/// A read half from [`KcpConn::split`]. Implements `kio::AsyncRead`; the
/// underlying state is already shared, so concurrent read/write is safe.
pub struct ReadHalf<'a> {
    inner: &'a KcpConn,
}

/// A write half from [`KcpConn::split`]. Implements `kio::AsyncWrite`.
pub struct WriteHalf<'a> {
    inner: &'a KcpConn,
}

/// An owned read half from [`KcpConn::into_split`]. The connection is closed
/// when the **last** owned half is dropped.
pub struct OwnedReadHalf {
    inner: KcpConn,
    _life: Lifecycle,
}

/// An owned write half from [`KcpConn::into_split`]. The connection is closed
/// when the **last** owned half is dropped.
pub struct OwnedWriteHalf {
    inner: KcpConn,
    _life: Lifecycle,
}

/// Close-on-last-half-drop guard: each owned half holds one `Lifecycle`, and
/// `remaining` (shared via `Arc`) starts at 2 — the last half to drop closes
/// the connection, so background loops don't leak when both halves are gone.
struct Lifecycle {
    shared: Arc<KcpConnShared>,
    remaining: Arc<AtomicUsize>,
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.close();
        }
    }
}

/// Internal accessor so the half trait impls share one code path.
trait HalfConn {
    fn conn(&self) -> &KcpConn;
}
impl<'a> HalfConn for ReadHalf<'a> {
    fn conn(&self) -> &KcpConn {
        self.inner
    }
}
impl<'a> HalfConn for WriteHalf<'a> {
    fn conn(&self) -> &KcpConn {
        self.inner
    }
}
impl HalfConn for OwnedReadHalf {
    fn conn(&self) -> &KcpConn {
        &self.inner
    }
}
impl HalfConn for OwnedWriteHalf {
    fn conn(&self) -> &KcpConn {
        &self.inner
    }
}

impl KcpConn {
    /// Split into borrowing read/write halves. Mirrors `tokio::net::TcpStream::split`.
    pub fn split(&self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        (ReadHalf { inner: self }, WriteHalf { inner: self })
    }

    /// Split into owned read/write halves. Mirrors
    /// `tokio::net::TcpStream::into_split`.
    ///
    /// The connection is closed when the **last** owned half is dropped (a
    /// shared [`Lifecycle`] refcount). The original `KcpConn` is consumed; its
    /// owner-`Drop` would otherwise close immediately.
    pub fn into_split(mut self) -> (OwnedReadHalf, OwnedWriteHalf) {
        self.owns_connection = false;
        let shared = self.shared.clone();
        let remaining = Arc::new(AtomicUsize::new(2));
        let read = OwnedReadHalf {
            inner: self.clone(),
            _life: Lifecycle {
                shared: shared.clone(),
                remaining: remaining.clone(),
            },
        };
        let write = OwnedWriteHalf {
            inner: self.clone(),
            _life: Lifecycle { shared, remaining },
        };
        // `self` drops here with owns_connection=false: no close, no leak.
        (read, write)
    }
}

macro_rules! impl_half_read {
    ($ty:ty) => {
        #[cfg(feature = "async-tokio")]
        impl kio::AsyncRead for $ty {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut kio::ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                let space = buf.initialize_unfilled();
                match self.conn().poll_read_into(cx, space) {
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

        #[cfg(feature = "async-smol")]
        impl kio::AsyncRead for $ty {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                self.conn().poll_read_into(cx, buf)
            }
        }
    };
}

macro_rules! impl_half_write {
    ($ty:ty) => {
        #[cfg(feature = "async-tokio")]
        impl kio::AsyncWrite for $ty {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.conn().do_poll_write(cx, buf)
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                self.conn().flush_notify_hint();
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                let _ = self.conn().shutdown(Shutdown::Write);
                Poll::Ready(Ok(()))
            }
        }

        #[cfg(feature = "async-smol")]
        impl kio::AsyncWrite for $ty {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.conn().do_poll_write(cx, buf)
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                self.conn().flush_notify_hint();
                Poll::Ready(Ok(()))
            }

            fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                let _ = self.conn().shutdown(Shutdown::Write);
                Poll::Ready(Ok(()))
            }
        }
    };
}

impl_half_read!(ReadHalf<'_>);
impl_half_write!(WriteHalf<'_>);
impl_half_read!(OwnedReadHalf);
impl_half_write!(OwnedWriteHalf);

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_buffer_tracks_bytes_and_entries() {
        let mut buffer = ReadBuffer::default();
        buffer.extend([Bytes::from_static(b"abc"), Bytes::from_static(b"defgh")]);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.bytes(), 8);

        let first = buffer.pop_front().unwrap();
        assert_eq!(first.as_ref(), b"abc");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.bytes(), 5);

        buffer.push_front(first.slice(1..));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.bytes(), 7);

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.bytes(), 0);
    }

    #[test]
    fn config_defaults_fast3ish() {
        let c = KcpConfig::default();
        assert_eq!(c.mtu, 1350);
        assert_eq!(c.sndwnd, 128);
        assert_eq!(c.rcvwnd, 128);
        assert!(matches!(c.mode, KcpMode::Fast3));
        assert!(c.stream);
        assert!(c.acknodelay);
        assert_eq!(c.datashard, 0);
        assert_eq!(c.parityshard, 0);
    }

    #[test]
    fn raw_packet_queue_recycles_capacity() {
        let mut q = RawPacketQueue::default();
        q.push(Bytes::from_static(b"a"));
        q.push(Bytes::from_static(b"b"));
        // drain hands out the batch with no lock-internal allocation.
        let batch = q.drain();
        assert_eq!(batch.len(), 2);
        assert!(q.is_empty());
        // recycle keeps the batch's capacity in the spare slot.
        q.recycle(batch);
        assert!(q.spare.capacity() >= 2);
        // The next burst accumulates into the recycled buffer (no re-grow).
        q.push(Bytes::from_static(b"c"));
        let batch2 = q.drain();
        assert_eq!(batch2.len(), 1);
        assert!(
            q.pending.capacity() >= 2,
            "recycled capacity should become the next accumulation target"
        );
        q.recycle(batch2);
    }

    #[test]
    fn raw_packet_queue_caps_retained_batch() {
        let mut q = RawPacketQueue::default();
        // A pathologically large batch is not retained (bounded memory).
        let big = Vec::with_capacity(MAX_RETAINED_RAW_BATCH + 1);
        q.recycle(big);
        assert!(q.spare.is_empty());
    }

    #[test]
    fn builder_sets_mtu_windows() {
        let b = KcpConn::connect("127.0.0.1:9")
            .mtu(1400)
            .sndwnd(256)
            .rcvwnd(64)
            .mode(KcpMode::Fast2)
            .stream(false)
            .acknodelay(false);
        assert_eq!(b.config.mtu, 1400);
        assert_eq!(b.config.sndwnd, 256);
        assert_eq!(b.config.rcvwnd, 64);
        assert!(matches!(b.config.mode, KcpMode::Fast2));
        assert!(!b.config.stream);
        assert!(!b.config.acknodelay);
    }

    #[test]
    fn apply_mode_values() {
        let mut kcp = KCP::new(1, 0, |_| {});
        let cfg = KcpConfig {
            mode: KcpMode::Fast3,
            mtu: 1350,
            ..KcpConfig::default()
        };
        kcp.apply(&cfg);
        assert_eq!(kcp.mtu(), 1350);
        assert_eq!(kcp.snd_wnd(), 128);
        assert_eq!(kcp.interval(), 10);
    }

    #[test]
    fn builder_fec_sets_shards() {
        let b = KcpConn::connect("127.0.0.1:9").fec(10, 3);
        assert_eq!(b.config.datashard, 10);
        assert_eq!(b.config.parityshard, 3);
    }
}

#[cfg(all(test, feature = "async-tokio"))]
mod integ {
    use super::*;
    use kio::AsyncReadExt;
    use kio::AsyncWriteExt;

    /// Two KcpConn over localhost UDP, bidirectional integrity check (no FEC).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bidirectional_localhost() {
        let (mut conn_a, mut conn_b) = pair_conns(None).await;
        roundtrip(&mut conn_a, &mut conn_b, b"hello-kcp-conn-phase1").await;
        roundtrip(&mut conn_b, &mut conn_a, b"ping-pong-reverse").await;
        conn_a.close();
        conn_b.close();
    }

    /// Bidirectional integrity with FEC 10/3 (Go-compatible defaults).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bidirectional_localhost_fec_10_3() {
        let (mut conn_a, mut conn_b) = pair_conns(Some((10, 3))).await;

        // Multi-packet payload so FEC groups fill (parity generated).
        let mut payload = vec![0u8; 32 * 1024];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        roundtrip(&mut conn_a, &mut conn_b, &payload).await;

        let mut payload2 = vec![0u8; 16 * 1024];
        for (i, b) in payload2.iter_mut().enumerate() {
            *b = (255 - (i % 251)) as u8;
        }
        roundtrip(&mut conn_b, &mut conn_a, &payload2).await;

        conn_a.close();
        conn_b.close();
    }

    /// Smaller FEC group (2/1) still preserves integrity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bidirectional_localhost_fec_2_1() {
        let (mut conn_a, mut conn_b) = pair_conns(Some((2, 1))).await;
        let payload = b"fec-2-1-small-payload-integrity-check!!!!";
        roundtrip(&mut conn_a, &mut conn_b, payload).await;
        roundtrip(&mut conn_b, &mut conn_a, b"reverse-2-1").await;
        conn_a.close();
        conn_b.close();
    }

    async fn pair_conns(fec: Option<(u32, u32)>) -> (KcpConn, KcpConn) {
        let a_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = kio::UdpSocket::connect(addr_a, addr_b).unwrap();
        let sock_b = kio::UdpSocket::connect(addr_b, addr_a).unwrap();

        let mut ba = KcpConn::with_transport(
            Arc::new(kio::DatagramSocket::Udp(sock_a)) as Arc<dyn PacketTransport>,
            addr_b,
        )
        .connected(true)
        .conv(0xC0FFEE)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(128)
        .rcvwnd(128);
        let mut bb = KcpConn::with_transport(
            Arc::new(kio::DatagramSocket::Udp(sock_b)) as Arc<dyn PacketTransport>,
            addr_a,
        )
        .connected(true)
        .conv(0xC0FFEE)
        .mode(KcpMode::Fast3)
        .mtu(1350)
        .sndwnd(128)
        .rcvwnd(128);
        if let Some((d, p)) = fec {
            ba = ba.fec(d, p);
            bb = bb.fec(d, p);
        }
        let conn_a = ba.build().await.unwrap();
        let conn_b = bb.build().await.unwrap();
        (conn_a, conn_b)
    }

    async fn roundtrip(from: &mut KcpConn, to: &mut KcpConn, payload: &[u8]) {
        from.write_all(payload).await.unwrap();
        from.flush().await.unwrap();
        let mut got = vec![0u8; payload.len()];
        read_exact_timeout(to, &mut got, Duration::from_secs(5)).await;
        assert_eq!(&got[..], payload);
    }

    async fn read_exact_timeout(conn: &mut KcpConn, buf: &mut [u8], limit: Duration) {
        let deadline = std::time::Instant::now() + limit;
        let mut filled = 0usize;
        while filled < buf.len() {
            if std::time::Instant::now() > deadline {
                panic!("timeout waiting for data, got {}/{}", filled, buf.len());
            }
            match kio::timeout(Duration::from_millis(50), conn.read(&mut buf[filled..])).await {
                Ok(Ok(0)) => panic!("unexpected EOF at {}", filled),
                Ok(Ok(n)) => filled += n,
                Ok(Err(e)) => panic!("read error: {}", e),
                Err(_) => continue,
            }
        }
    }
}
