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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
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

/// FEC header + SIZE field (`fecHeaderSizePlus2` in Go).
const FEC_HDR: usize = FEC_HEADER_SIZE_PLUS_2;

/// Max UDP datagram size for the input loop recv buffer.
const MAX_DATAGRAM: usize = 2048;
/// Upper bound on flush-loop sleep when idle (ms).
const KCP_UPDATE_INTERVAL_MS: u64 = 2;

// ─── PacketTransport ──────────────────────────────────────────────────────────

/// Pluggable datagram layer under [`KcpConn`].
///
/// Implementations: [`kio::DatagramSocket`] (plain UDP / TcpRaw) and
/// `kcptun_common::CryptoTransport` (encrypt/decrypt wrapper).
///
/// Async methods return boxed futures so the trait is object-safe without
/// the `async_trait` crate (surgical, no new workspace dep).
pub trait PacketTransport: Send + Sync {
    /// Read one datagram into `buf`. Returns bytes written.
    fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    /// Non-blocking read; `WouldBlock` when nothing ready.
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Batch-send on a connected socket.
    fn send_batch<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    /// Batch-send to an explicit peer (unconnected socket).
    fn send_batch_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    /// High-priority send (ACK path). Default = [`send_batch`](Self::send_batch).
    /// Crypto wrappers use a separate buffer here to avoid lock contention.
    fn send_urgent<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        self.send_batch(packets)
    }

    /// High-priority send_to (ACK path, unconnected). Default = send_batch_to.
    fn send_urgent_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        self.send_batch_to(packets, target)
    }

    fn local_addr(&self) -> io::Result<SocketAddr>;
}

impl PacketTransport for kio::DatagramSocket {
    fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        // Call inherent method (not trait) to avoid recursion.
        Box::pin(async move { kio::DatagramSocket::recv(self, buf).await })
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        kio::DatagramSocket::try_recv(self, buf)
    }

    fn send_batch<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move { kio::DatagramSocket::send_batch(self, packets).await })
    }

    fn send_batch_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move { kio::DatagramSocket::send_batch_to(self, packets, target).await })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        kio::DatagramSocket::local_addr(self)
    }
}

// ─── Shared state ─────────────────────────────────────────────────────────────

struct KcpConnShared {
    transport: Arc<dyn PacketTransport>,
    kcp: Arc<Mutex<KCP>>,
    write_buf: Mutex<BytesMut>,
    read_buf: Mutex<VecDeque<Bytes>>,
    raw_packets: Arc<Mutex<Vec<Bytes>>>,
    flush_notify: Arc<kio::Notify>,
    write_notify: Arc<kio::Notify>,
    read_notify: Arc<kio::Notify>,
    read_waker: Mutex<Option<Waker>>,
    wait_send: Arc<AtomicUsize>,
    snd_wnd: usize,
    /// `snd_wnd * MSS` — hard cap on buffered-but-unsent user bytes. Without it,
    /// `poll_write` gates only on the flush-loop-cached `wait_send`, and a fast
    /// writer overshoots `write_buf` before the cache updates (M0.1).
    window_bytes: usize,
    acknodelay: bool,
    remote_addr: SocketAddr,
    /// When true, use `send_batch` / `send_urgent` (connected). Else `*_to(remote)`.
    connected: bool,
    closed: Arc<AtomicBool>,
    /// Ensures at most one backpressure waiter is armed for poll_write.
    bp_armed: AtomicBool,
    /// Optional FEC encoder (header_offset=0, matching client/server session layout).
    fec_encoder: Option<Mutex<FecEncoder>>,
    /// Optional FEC decoder.
    fec_decoder: Option<Mutex<FecDecoder>>,
}

impl KcpConnShared {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Backpressure has room: either the send window is below `snd_wnd` (ACKs
    /// are flowing) or the buffered-unsent `write_buf` has drained below the
    /// window cap (the flush loop consumed it into `snd_queue`).
    fn backpressure_relieved(&self) -> bool {
        if self.wait_send.load(Ordering::Relaxed) < self.snd_wnd {
            return true;
        }
        self.write_buf.lock().len() < self.window_bytes
    }

    fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.flush_notify.notify_one();
            self.write_notify.notify_waiters();
            self.read_notify.notify_waiters();
            if let Some(w) = self.read_waker.lock().take() {
                w.wake();
            }
        }
    }

    fn wake_reader(&self) {
        self.read_notify.notify_waiters();
        if let Some(w) = self.read_waker.lock().take() {
            w.wake();
        }
    }

    fn drain_raw_packets(&self) -> Vec<Bytes> {
        let mut g = self.raw_packets.lock();
        let n = g.len();
        let cap = g.capacity();
        let p = std::mem::take(&mut *g);
        if cap < n {
            g.reserve(n - cap);
        }
        p
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

    /// ACK / high-priority path — uses `send_urgent` so crypto can take a
    /// separate buffer without contending with the data encrypt path.
    async fn send_urgent_packets(&self, packets: &[Bytes]) -> io::Result<()> {
        if packets.is_empty() {
            return Ok(());
        }
        if self.connected {
            self.transport.send_urgent(packets).await
        } else {
            self.transport
                .send_urgent_to(packets, self.remote_addr)
                .await
        }
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
            },
            Err(e) => KcpConnBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::Udp,
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
            },
            Err(e) => KcpConnBuilder {
                remote: None,
                transport: None,
                config: KcpConfig::default(),
                connected: true,
                resolve_err: Some(e),
                dial: DialTransport::TcpRaw,
            },
        }
    }

    /// Dynamic nodelay tweak after construction.
    pub fn set_nodelay(&self, nodelay: u32, interval: u32, resend: u32, nc: u32) {
        self.shared
            .kcp
            .lock()
            .set_nodelay(nodelay, interval, resend, nc);
    }

    pub fn set_window_size(&self, snd_wnd: u32, rcv_wnd: u32) {
        let mut kcp = self.shared.kcp.lock();
        kcp.set_snd_wnd(snd_wnd);
        kcp.set_rcv_wnd(rcv_wnd);
    }

    pub fn set_mtu(&self, mtu: u32) {
        self.shared.kcp.lock().set_mtu(mtu);
    }

    pub fn set_stream_mode(&self, enable: bool) {
        self.shared.kcp.lock().set_stream_mode(enable);
    }

    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.shared.remote_addr
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.transport.local_addr()
    }

    /// Configured send window (for diagnostics / backpressure).
    pub fn snd_wnd(&self) -> usize {
        self.shared.snd_wnd
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

    /// Async read borrowing `&self` — safe for **concurrent** read/write tasks
    /// (the internal state is already shared behind mutexes/atomics).
    ///
    /// Mirrors `poll_read_into` semantics without needing `Pin<&mut Self>`.
    /// Waits on an internal notify when `read_buf` is empty (2ms poll slice).
    pub async fn read_shared(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if buf.is_empty() {
                return Ok(0);
            }
            {
                let mut rb = self.shared.read_buf.lock();
                if let Some(mut data) = rb.pop_front() {
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    if n < data.len() {
                        let _ = data.split_to(n);
                        rb.push_front(data);
                    }
                    return Ok(n);
                }
            }
            if self.shared.is_closed() {
                return Ok(0);
            }
            let _ =
                kio::timeout(Duration::from_millis(2), self.shared.read_notify.notified()).await;
        }
    }

    /// Async `write_all` borrowing `&self` — safe for concurrent read/write.
    ///
    /// Mirrors `do_poll_write`'s backpressure (send window + `write_buf` cap)
    /// and waits on the internal write notify when blocked.
    pub async fn write_all_shared(&self, buf: &[u8]) -> io::Result<()> {
        let mut offset = 0usize;
        while offset < buf.len() {
            if self.shared.is_closed() {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "KcpConn closed"));
            }
            let ws = self.shared.wait_send.load(Ordering::Relaxed);
            if ws >= self.shared.snd_wnd {
                let _ = kio::timeout(
                    Duration::from_millis(2),
                    self.shared.write_notify.notified(),
                )
                .await;
                continue;
            }
            let wbuf_len = self.shared.write_buf.lock().len();
            let max_accept = self.shared.window_bytes.saturating_sub(wbuf_len);
            if max_accept == 0 {
                let _ = kio::timeout(
                    Duration::from_millis(2),
                    self.shared.write_notify.notified(),
                )
                .await;
                continue;
            }
            let n = (buf.len() - offset).min(max_accept);
            {
                let mut wb = self.shared.write_buf.lock();
                wb.extend_from_slice(&buf[offset..offset + n]);
            }
            offset += n;
            self.shared.flush_notify.notify_one();
        }
        Ok(())
    }
}

impl Drop for KcpConn {
    fn drop(&mut self) {
        self.shared.close();
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

/// Builder for [`KcpConn`]. Call [`.build().await`](Self::build) to construct.
pub struct KcpConnBuilder {
    remote: Option<SocketAddr>,
    transport: Option<Arc<dyn PacketTransport>>,
    config: KcpConfig,
    connected: bool,
    resolve_err: Option<io::Error>,
    dial: DialTransport,
}

impl KcpConnBuilder {
    pub fn mtu(mut self, v: u32) -> Self {
        self.config.mtu = v;
        self
    }

    pub fn sndwnd(mut self, v: u32) -> Self {
        self.config.sndwnd = v;
        self
    }

    pub fn rcvwnd(mut self, v: u32) -> Self {
        self.config.rcvwnd = v;
        self
    }

    pub fn mode(mut self, v: KcpMode) -> Self {
        self.config.mode = v;
        self
    }

    pub fn stream(mut self, v: bool) -> Self {
        self.config.stream = v;
        self
    }

    pub fn acknodelay(mut self, v: bool) -> Self {
        self.config.acknodelay = v;
        self
    }

    pub fn conv(mut self, v: u32) -> Self {
        self.config.conv = v;
        self
    }

    pub fn token(mut self, v: u32) -> Self {
        self.config.token = v;
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

    /// Whether the transport is already `connect()`ed (use `send` / `send_batch`).
    ///
    /// Default: `true` for [`KcpConn::connect`], `false` for [`KcpConn::with_transport`].
    pub fn connected(mut self, v: bool) -> Self {
        self.connected = v;
        self
    }

    /// Enable Reed-Solomon FEC (`datashard` / `parityshard`, both must be > 0).
    ///
    /// Matches client/server session layout (`header_offset=0`).
    pub fn fec(mut self, datashard: u32, parityshard: u32) -> Self {
        self.config.datashard = datashard;
        self.config.parityshard = parityshard;
        self
    }

    pub fn config(mut self, cfg: KcpConfig) -> Self {
        self.config = cfg;
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
        let raw_packets = Arc::new(Mutex::new(Vec::<Bytes>::new()));
        let raw_packets_cb = raw_packets.clone();

        let mut kcp = KCP::new(
            config.conv,
            config.token,
            Box::new(move |data: Bytes| {
                raw_packets_cb.lock().push(data);
            }),
        );
        kcp.apply(&config);

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

        // Capture before `kcp` moves into the shared Arc (M0.1 write_buf cap).
        let mss = kcp.mss() as usize;

        let shared = Arc::new(KcpConnShared {
            transport,
            kcp: Arc::new(Mutex::new(kcp)),
            write_buf: Mutex::new(BytesMut::with_capacity(64 * 1024)),
            read_buf: Mutex::new(VecDeque::new()),
            raw_packets,
            flush_notify: Arc::new(kio::Notify::new()),
            write_notify: Arc::new(kio::Notify::new()),
            read_notify: Arc::new(kio::Notify::new()),
            read_waker: Mutex::new(None),
            wait_send: Arc::new(AtomicUsize::new(0)),
            snd_wnd: config.sndwnd as usize,
            window_bytes: (config.sndwnd as usize).saturating_mul(mss).max(1),
            acknodelay: config.acknodelay,
            remote_addr: remote,
            connected,
            closed: Arc::new(AtomicBool::new(false)),
            bp_armed: AtomicBool::new(false),
            fec_encoder,
            fec_decoder,
        });

        let handles = vec![
            spawn_input_loop(shared.clone()),
            spawn_flush_loop(shared.clone()),
        ];

        Ok(KcpConn {
            shared,
            _handles: handles,
        })
    }
}

fn resolve_one(addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve address"))
}

// ─── Background loops ─────────────────────────────────────────────────────────

fn spawn_input_loop(shared: Arc<KcpConnShared>) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            if shared.is_closed() {
                break;
            }
            let n = match shared.transport.recv(&mut buf).await {
                Ok(n) if n > 0 => n,
                Ok(_) => continue,
                Err(_) if shared.is_closed() => break,
                Err(_) => {
                    kio::sleep_ms(10).await;
                    continue;
                }
            };

            let mut n = n;
            loop {
                if n > 0 {
                    feed_inbound(&shared, &buf[..n]);
                    shared.wake_reader();

                    let acks = shared.drain_raw_packets();
                    if !acks.is_empty() {
                        let wire = maybe_fec_expand(&shared, &acks);
                        let _ = shared.send_urgent_packets(&wire).await;
                    }
                }

                match shared.transport.try_recv(&mut buf) {
                    Ok(m) if m > 0 => n = m,
                    _ => break,
                }
            }
        }
    })
}

/// Feed one inbound datagram into KCP (with optional FEC decode).
///
/// Mirrors kcptun-client FEC handling:
/// - type 0x00f1 (data): input payload after FEC_HDR; also recovered via
///   `fec_kcp_from_recovered`
/// - type 0x00f2 (parity): recovered only
/// - else: fallback raw KCP input if long enough
fn feed_inbound(shared: &KcpConnShared, input: &[u8]) {
    let mut kcp = shared.kcp.lock();
    let mut had_input = false;

    if let Some(ref dec) = shared.fec_decoder {
        if input.len() >= 6 {
            let fec_flag = u16::from_le_bytes([input[4], input[5]]);
            let recovered = {
                let mut d = dec.lock();
                d.decode(input)
            };
            match fec_flag {
                FEC_TYPE_DATA => {
                    if input.len() > FEC_HDR {
                        if kcp.input(&input[FEC_HDR..], shared.acknodelay).is_ok() {
                            had_input = true;
                        }
                    }
                    for r in &recovered {
                        if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                            if kcp.input(kcp_slice, shared.acknodelay).is_ok() {
                                had_input = true;
                            }
                        }
                    }
                }
                FEC_TYPE_PARITY => {
                    for r in &recovered {
                        if let Some(kcp_slice) = fec_kcp_from_recovered(r) {
                            if kcp.input(kcp_slice, shared.acknodelay).is_ok() {
                                had_input = true;
                            }
                        }
                    }
                }
                _ => {
                    // Unknown FEC type / non-FEC frame: try raw KCP if long enough.
                    if input.len() >= 24 && kcp.input(input, shared.acknodelay).is_ok() {
                        had_input = true;
                    }
                }
            }
        } else if input.len() >= 24 {
            if kcp.input(input, shared.acknodelay).is_ok() {
                had_input = true;
            }
        }
    } else if input.len() >= 24 {
        if kcp.input(input, shared.acknodelay).is_ok() {
            had_input = true;
        }
    }

    if had_input {
        while let Ok(d) = kcp.recv_bytes() {
            if !d.is_empty() {
                shared.read_buf.lock().push_back(d);
            }
        }
    }

    let ws = kcp.wait_send() as usize;
    shared.wait_send.store(ws, Ordering::Relaxed);
    if ws < shared.snd_wnd {
        shared.write_notify.notify_waiters();
    }
}

/// FEC-expand raw KCP segments when encoder is present; else identity.
fn maybe_fec_expand(shared: &KcpConnShared, packets: &[Bytes]) -> Vec<Bytes> {
    if let Some(ref enc) = shared.fec_encoder {
        let mut e = enc.lock();
        fec_expand_packets(&mut e, packets, 500)
    } else {
        packets.to_vec()
    }
}

fn spawn_flush_loop(shared: Arc<KcpConnShared>) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        let mut next_update: u64 = KCP_UPDATE_INTERVAL_MS;
        // Dead-link probe every ~100ms (loop wakes every 1-2ms under load):
        // tear down so blocked readers/writers unblock on a silent peer (M1-A).
        let mut dead_checks: u32 = 50;
        loop {
            if shared.is_closed() {
                break;
            }

            let _ = kio::timeout(
                Duration::from_millis(next_update),
                shared.flush_notify.notified(),
            )
            .await;

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

            let ws = {
                let pending = {
                    let mut wb = shared.write_buf.lock();
                    if wb.is_empty() {
                        None
                    } else {
                        Some(wb.split().freeze())
                    }
                };

                let mut kcp = shared.kcp.lock();
                let mut had = false;
                if let Some(data) = pending {
                    if !data.is_empty() {
                        had = true;
                        let mss = kcp.mss() as usize;
                        let max_chunk = (KCP_MAX_FRAG as usize)
                            .saturating_sub(1)
                            .saturating_mul(mss)
                            .max(mss);
                        let mut offset = 0;
                        while offset < data.len() {
                            let end = (offset + max_chunk).min(data.len());
                            if kcp.send(&data[offset..end]).is_err() {
                                break;
                            }
                            offset = end;
                        }
                    }
                }
                next_update = kcp.flush() as u64;
                let ws = kcp.wait_send() as usize;
                if had || ws > 0 {
                    next_update = 1;
                } else {
                    next_update = next_update.clamp(1, KCP_UPDATE_INTERVAL_MS);
                }
                ws
            };

            shared.wait_send.store(ws, Ordering::Relaxed);
            shared.write_notify.notify_waiters();

            let packets = shared.drain_raw_packets();
            if !packets.is_empty() {
                let wire = maybe_fec_expand(&shared, &packets);
                let _ = shared.send_packets(&wire).await;
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
        {
            let mut rb = self.shared.read_buf.lock();
            if let Some(mut data) = rb.pop_front() {
                let n = data.len().min(out.len());
                out[..n].copy_from_slice(&data[..n]);
                if n < data.len() {
                    let _ = data.split_to(n);
                    rb.push_front(data);
                }
                return Poll::Ready(Ok(n));
            }
        }
        if self.shared.is_closed() {
            return Poll::Ready(Ok(0));
        }
        *self.shared.read_waker.lock() = Some(cx.waker().clone());
        {
            let mut rb = self.shared.read_buf.lock();
            if let Some(mut data) = rb.pop_front() {
                let n = data.len().min(out.len());
                out[..n].copy_from_slice(&data[..n]);
                if n < data.len() {
                    let _ = data.split_to(n);
                    rb.push_front(data);
                }
                return Poll::Ready(Ok(n));
            }
        }
        if self.shared.is_closed() {
            return Poll::Ready(Ok(0));
        }
        Poll::Pending
    }

    fn arm_backpressure_wake(&self, cx: &mut Context<'_>) {
        let waker = cx.waker().clone();
        if self
            .shared
            .bp_armed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            let shared = self.shared.clone();
            kio::spawn_task(async move {
                kio::sleep_ms(1).await;
                if shared.backpressure_relieved() || !shared.bp_armed.load(Ordering::Acquire) {
                    waker.wake();
                }
            });
            return;
        }
        let shared = self.shared.clone();
        kio::spawn_task(async move {
            loop {
                let _ =
                    kio::timeout(Duration::from_millis(2), shared.write_notify.notified()).await;
                if shared.backpressure_relieved() || shared.is_closed() {
                    shared.bp_armed.store(false, Ordering::Release);
                    waker.wake();
                    return;
                }
            }
        });
    }

    fn do_poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.shared.is_closed() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "KcpConn closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let ws = self.shared.wait_send.load(Ordering::Relaxed);
        if ws >= self.shared.snd_wnd {
            self.arm_backpressure_wake(cx);
            return Poll::Pending;
        }
        // Bound buffered-but-unsent bytes so a fast writer can't overshoot
        // `write_buf` past one send window (M0.1). Accept only what fits; the
        // caller (`write_all` / flush task) retries the remainder once the
        // flush loop drains.
        let wbuf_len = {
            let wb = self.shared.write_buf.lock();
            wb.len()
        };
        let max_accept = self.shared.window_bytes.saturating_sub(wbuf_len);
        if max_accept == 0 {
            self.arm_backpressure_wake(cx);
            return Poll::Pending;
        }
        let n = buf.len().min(max_accept);
        {
            let mut wb = self.shared.write_buf.lock();
            wb.extend_from_slice(&buf[..n]);
        }
        self.shared.flush_notify.notify_one();
        Poll::Ready(Ok(n))
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
        self.close();
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
        self.close();
        Poll::Ready(Ok(()))
    }
}

// ─── KcpListener (multi-peer server) ─────────────────────────────────────────

/// Per-peer inbound queue + wakeup used by [`KcpListener`] to feed one shared
/// bound socket's datagrams into each accepted [`KcpConn`].
struct PeerQueue {
    packets: Mutex<VecDeque<Bytes>>,
    notify: kio::Notify,
    closed: AtomicBool,
}

impl PeerQueue {
    fn new() -> Self {
        Self {
            packets: Mutex::new(VecDeque::new()),
            notify: kio::Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn push(&self, pkt: Bytes) {
        self.packets.lock().push_back(pkt);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<Bytes> {
        self.packets.lock().pop_front()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

/// `PacketTransport` for one accepted peer: reads inbound from its
/// [`PeerQueue`] and writes outbound on the shared listen socket addressed to
/// that peer.
///
/// Dropping the transport (i.e. dropping the accepted `KcpConn`) closes the
/// peer queue so the listener reaps it and can accept a fresh connection from
/// the same address.
struct PeerTransport {
    queue: Arc<PeerQueue>,
    socket: Arc<kio::DatagramSocket>,
    peer: SocketAddr,
}

impl PacketTransport for PeerTransport {
    fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                if let Some(pkt) = self.queue.pop() {
                    let n = pkt.len().min(buf.len());
                    buf[..n].copy_from_slice(&pkt[..n]);
                    return Ok(n);
                }
                if self.queue.is_closed() {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "KcpConn: peer session closed",
                    ));
                }
                // Arm the notification, then re-check to close the wake race.
                let notified = self.queue.notify.notified();
                if let Some(pkt) = self.queue.pop() {
                    let n = pkt.len().min(buf.len());
                    buf[..n].copy_from_slice(&pkt[..n]);
                    return Ok(n);
                }
                if self.queue.is_closed() {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "KcpConn: peer session closed",
                    ));
                }
                notified.await;
            }
        })
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self.queue.pop() {
            Some(pkt) => {
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok(n)
            }
            None => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "peer queue empty",
            )),
        }
    }

    fn send_batch<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move { self.socket.send_batch_to(packets, self.peer).await })
    }

    fn send_batch_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        _target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move { self.socket.send_batch_to(packets, self.peer).await })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Drop for PeerTransport {
    fn drop(&mut self) {
        self.queue.mark_closed();
    }
}

/// KCP server listener: binds one UDP socket and demultiplexes inbound
/// datagrams by source address, exposing each peer as a [`KcpConn`] via
/// [`accept`](KcpListener::accept).
pub struct KcpListener {
    socket: Arc<kio::DatagramSocket>,
    pending: Arc<Mutex<VecDeque<(KcpConn, SocketAddr)>>>,
    accept_notify: Arc<kio::Notify>,
    closed: Arc<AtomicBool>,
    _reader: kio::JoinHandle<()>,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        self.close();
    }
}

impl KcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpListenerBuilder {
        match resolve_one(addr) {
            Ok(a) => KcpListenerBuilder {
                addr: Some(a),
                config: KcpConfig::default(),
                resolve_err: None,
            },
            Err(e) => KcpListenerBuilder {
                addr: None,
                config: KcpConfig::default(),
                resolve_err: Some(e),
            },
        }
    }

    /// Local address of the listen socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accept the next client connection, returning the per-peer `KcpConn` and
    /// the peer's `SocketAddr`. Returns `ConnectionAborted` once closed.
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)> {
        loop {
            if let Some(v) = self.pending.lock().pop_front() {
                return Ok(v);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "KcpListener closed",
                ));
            }
            let notified = self.accept_notify.notified();
            if let Some(v) = self.pending.lock().pop_front() {
                return Ok(v);
            }
            notified.await;
        }
    }

    /// Stop accepting new connections. Existing accepted [`KcpConn`]s are
    /// unaffected; the reader task exits on its next tick.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.accept_notify.notify_waiters();
        }
    }
}

/// Builder for [`KcpListener`].
pub struct KcpListenerBuilder {
    addr: Option<SocketAddr>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
}

impl KcpListenerBuilder {
    pub fn mtu(mut self, v: u32) -> Self {
        self.config.mtu = v;
        self
    }

    pub fn sndwnd(mut self, v: u32) -> Self {
        self.config.sndwnd = v;
        self
    }

    pub fn rcvwnd(mut self, v: u32) -> Self {
        self.config.rcvwnd = v;
        self
    }

    pub fn mode(mut self, v: KcpMode) -> Self {
        self.config.mode = v;
        self
    }

    pub fn stream(mut self, v: bool) -> Self {
        self.config.stream = v;
        self
    }

    pub fn acknodelay(mut self, v: bool) -> Self {
        self.config.acknodelay = v;
        self
    }

    pub fn conv(mut self, v: u32) -> Self {
        self.config.conv = v;
        self
    }

    pub fn token(mut self, v: u32) -> Self {
        self.config.token = v;
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

    pub fn fec(mut self, d: u32, p: u32) -> Self {
        self.config.datashard = d;
        self.config.parityshard = p;
        self
    }

    pub fn config(mut self, cfg: KcpConfig) -> Self {
        self.config = cfg;
        self
    }

    /// Bind the listen socket, spawn the demux reader, and return the listener.
    pub async fn build(self) -> io::Result<KcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let addr = self.addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpListener: bind address required",
            )
        })?;
        let udp = kio::UdpSocket::bind(addr)?;
        let socket = Arc::new(kio::DatagramSocket::Udp(udp));

        let sessions = Arc::new(Mutex::new(HashMap::<SocketAddr, Arc<PeerQueue>>::new()));
        let pending = Arc::new(Mutex::new(VecDeque::<(KcpConn, SocketAddr)>::new()));
        let accept_notify = Arc::new(kio::Notify::new());
        let closed = Arc::new(AtomicBool::new(false));

        let reader = spawn_listener_reader(
            socket.clone(),
            self.config,
            sessions.clone(),
            pending.clone(),
            accept_notify.clone(),
            closed.clone(),
        );

        Ok(KcpListener {
            socket,
            pending,
            accept_notify,
            closed,
            _reader: reader,
        })
    }
}

/// Reader task: `recv_from` the shared listen socket, demux by source address,
/// and feed each peer's inbound queue. New / reconnecting peers get a fresh
/// [`KcpConn`] pushed onto the accept queue.
fn spawn_listener_reader(
    socket: Arc<kio::DatagramSocket>,
    config: KcpConfig,
    sessions: Arc<Mutex<HashMap<SocketAddr, Arc<PeerQueue>>>>,
    pending: Arc<Mutex<VecDeque<(KcpConn, SocketAddr)>>>,
    accept_notify: Arc<kio::Notify>,
    closed: Arc<AtomicBool>,
) -> kio::JoinHandle<()> {
    kio::spawn_task(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            if closed.load(Ordering::Acquire) {
                break;
            }
            let (n, peer) =
                match kio::timeout(Duration::from_millis(100), socket.recv_from(&mut buf)).await {
                    Ok(Ok(v)) => v,
                    Ok(Err(_)) => {
                        kio::sleep_ms(10).await;
                        continue;
                    }
                    Err(_) => continue, // 100ms tick → re-check closed
                };

            // Get-or-create this peer's queue. The reader is single-threaded,
            // so get-or-create cannot race with itself.
            let queue = {
                let mut sessions = sessions.lock();
                match sessions.get(&peer) {
                    Some(q) if !q.is_closed() => Some(q.clone()),
                    _ => {
                        sessions.remove(&peer);
                        None
                    }
                }
            };
            let queue = match queue {
                Some(q) => q,
                None => {
                    let queue = Arc::new(PeerQueue::new());
                    let transport = Arc::new(PeerTransport {
                        queue: queue.clone(),
                        socket: socket.clone(),
                        peer,
                    });
                    match KcpConn::with_transport(transport, peer)
                        .connected(false)
                        .config(config.clone())
                        .build()
                        .await
                    {
                        Ok(conn) => {
                            sessions.lock().insert(peer, queue.clone());
                            pending.lock().push_back((conn, peer));
                            accept_notify.notify_one();
                            queue
                        }
                        Err(_) => {
                            queue.mark_closed();
                            continue; // drop this datagram
                        }
                    }
                }
            };
            queue.push(Bytes::copy_from_slice(&buf[..n]));
        }
    })
}

// ─── KcpTcpListener (server, 1 TCP conn = 1 KCP session) ─────────────────────

/// TCP-mode KCP server listener: each accepted raw-TCP connection becomes its
/// own [`KcpConn`]. Linux only (`kio::TcpRawListener`); non-Linux bind returns
/// `io::Unsupported`.
pub struct KcpTcpListener {
    listener: kio::TcpRawListener,
    config: KcpConfig,
    closed: AtomicBool,
}

impl Drop for KcpTcpListener {
    fn drop(&mut self) {
        self.close();
    }
}

impl KcpTcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpTcpListenerBuilder {
        match resolve_one(addr) {
            Ok(a) => KcpTcpListenerBuilder {
                addr: Some(a),
                config: KcpConfig::default(),
                resolve_err: None,
            },
            Err(e) => KcpTcpListenerBuilder {
                addr: None,
                config: KcpConfig::default(),
                resolve_err: Some(e),
            },
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next client connection: one [`KcpConn`] per accepted TCP
    /// connection. Returns `ConnectionAborted` once closed.
    pub async fn accept(&self) -> io::Result<(KcpConn, SocketAddr)> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "KcpTcpListener closed",
            ));
        }
        let (conn, peer) = self.listener.accept().await?;
        let socket: Arc<dyn PacketTransport> = Arc::new(kio::DatagramSocket::TcpRaw(conn));
        let kcp = KcpConn::with_transport(socket, peer)
            .connected(true)
            .config(self.config.clone())
            .build()
            .await?;
        Ok((kcp, peer))
    }

    /// Stop accepting new connections. Existing accepted [`KcpConn`]s are
    /// unaffected (they hold their own raw-fd Arc).
    ///
    /// Limitation: this only flips the internal `closed` flag; it does NOT
    /// abort an `accept()` that is already blocked in the kernel
    /// (`TcpRawListener::accept` runs a blocking `accept(2)` inside
    /// `cpu_block`). A blocked `accept()` only returns once the underlying
    /// listener is dropped (e.g. when this `KcpTcpListener` goes out of scope),
    /// at which point the listener fd is closed and the kernel unblocks it.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {}
    }
}

/// Builder for [`KcpTcpListener`].
pub struct KcpTcpListenerBuilder {
    addr: Option<SocketAddr>,
    config: KcpConfig,
    resolve_err: Option<io::Error>,
}

impl KcpTcpListenerBuilder {
    pub fn config(mut self, cfg: KcpConfig) -> Self {
        self.config = cfg;
        self
    }

    /// Bind the raw-TCP listener and return it.
    pub fn build(self) -> io::Result<KcpTcpListener> {
        if let Some(e) = self.resolve_err {
            return Err(e);
        }
        let addr = self.addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KcpTcpListener: bind address required",
            )
        })?;
        let listener = kio::tcpraw_listen(&addr)?;
        Ok(KcpTcpListener {
            listener,
            config: self.config,
            closed: AtomicBool::new(false),
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut kcp = KCP::new(1, 0, Box::new(|_| {}));
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
