//! TCP raw socket transport emulation (Linux only).
//!
//! Uses `TCP_REPAIR` to silence the kernel TCP stack after the three-way
//! handshake, then sends/receives **TCP segments** via a raw socket
//! (`SOCK_RAW` + `IPPROTO_TCP`, **no** `IP_HDRINCL`).
//!
//! This matches Go `xtaci/tcpraw` wire shape on the raw socket:
//!   - send: `[TCP Header 20B + 12B Timestamp Options][KCP Datagram]`
//!   - recv: same (kernel strips/adds the IP header)
//!
//! Go reference: `vendor/github.com/xtaci/tcpraw/tcp_linux.go`

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;

use socket2::Socket;

// ─── TCP_REPAIR / queue constants (Linux uapi) ───────────────────────────────
const TCP_REPAIR: i32 = 19;
const TCP_REPAIR_QUEUE: i32 = 20;
const TCP_QUEUE_SEQ: i32 = 21;
const TCP_RECV_QUEUE: i32 = 1;
const TCP_SEND_QUEUE: i32 = 2;

// ─── TCP fingerprint (matching Go tcpraw fingerPrintLinux) ───────────────────
const TCP_WINDOW: u16 = 65535;
const MAX_PAYLOAD: usize = 1400;
const RAW_RECV_BUF: usize = 2 * 1024 * 1024;
const CHANNEL_CAP: usize = 256;
const TCP_HDR_LEN: usize = 32; // 20 base + 12 timestamp options

/// Initial TCP flow state captured after the three-way handshake.
///
/// Uses `TCP_REPAIR` + `TCP_QUEUE_SEQ` rather than extended `tcp_info` fields
/// (absent from the `libc` crate). Peer timestamp echo (`ts_ecr`) starts at 0
/// and is filled from captured TCP options.
struct RepairState {
    seq: u32,
    ack: u32,
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn socket_addr_to_ipv4(addr: &SocketAddr) -> io::Result<([u8; 4], u16)> {
    match addr {
        SocketAddr::V4(v4) => Ok((v4.ip().octets(), v4.port())),
        SocketAddr::V6(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tcpraw only supports IPv4",
        )),
    }
}

fn set_tcp_repair(stream: &std::net::TcpStream, on: bool) -> io::Result<()> {
    let val: libc::c_int = if on { 1 } else { 0 };
    let ret = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_TCP,
            TCP_REPAIR,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn set_repair_queue(stream: &std::net::TcpStream, queue: i32) -> io::Result<()> {
    let val: libc::c_int = queue;
    let ret = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_TCP,
            TCP_REPAIR_QUEUE,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn get_queue_seq(stream: &std::net::TcpStream) -> io::Result<u32> {
    let mut seq: u32 = 0;
    let mut len = std::mem::size_of::<u32>() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_TCP,
            TCP_QUEUE_SEQ,
            &mut seq as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(seq)
}

/// Enter TCP_REPAIR and capture post-handshake seq/ack state.
fn capture_repair_state(stream: &std::net::TcpStream) -> io::Result<RepairState> {
    set_tcp_repair(stream, true)?;

    set_repair_queue(stream, TCP_SEND_QUEUE)?;
    let seq = get_queue_seq(stream)?;

    set_repair_queue(stream, TCP_RECV_QUEUE)?;
    let ack = get_queue_seq(stream)?;

    Ok(RepairState { seq, ack })
}

/// Open `SOCK_RAW` + `IPPROTO_TCP` **without** `IP_HDRINCL`.
///
/// Matches Go `net.DialIP("ip:tcp", …)` / `ListenIP("ip:tcp", …)`:
/// userspace supplies TCP segments only; the kernel adds/strips IP headers.
fn open_raw_tcp_socket() -> io::Result<OwnedFd> {
    let raw = Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::RAW,
        Some(socket2::Protocol::from(libc::IPPROTO_TCP)),
    )?;
    let _ = raw.set_recv_buffer_size(RAW_RECV_BUF);
    let _ = raw.set_send_buffer_size(RAW_RECV_BUF);
    Ok(unsafe { OwnedFd::from_raw_fd(raw.into_raw_fd()) })
}

// ─── Checksum ────────────────────────────────────────────────────────────────

fn ones_complement_sum(data: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    let chunks = data.chunks_exact(2);
    let remainder = chunks.remainder();
    for chunk in chunks {
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    if remainder.len() == 1 {
        sum += (remainder[0] as u32) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    sum
}

fn tcp_checksum(src_ip: &[u8; 4], dst_ip: &[u8; 4], tcp_segment: &[u8]) -> u16 {
    let mut buf = Vec::with_capacity(12 + tcp_segment.len() + 1);
    buf.extend_from_slice(src_ip);
    buf.extend_from_slice(dst_ip);
    buf.push(0);
    buf.push(6); // TCP
    let tcp_len = tcp_segment.len() as u16;
    buf.extend_from_slice(&tcp_len.to_be_bytes());
    buf.extend_from_slice(tcp_segment);
    if buf.len() % 2 != 0 {
        buf.push(0);
    }
    !ones_complement_sum(&buf) as u16
}

// ─── Packet Construction ─────────────────────────────────────────────────────

/// Build a TCP segment only (no IP header) — Go tcpraw / IPPROTO_TCP raw shape.
fn build_tcp_segment(
    src_ip: &[u8; 4],
    dst_ip: &[u8; 4],
    src_port: u16,
    dst_port: u16,
    seq: u32,
    ack: u32,
    ts_val: u32,
    ts_ecr: u32,
    payload: &[u8],
) -> Vec<u8> {
    let total = TCP_HDR_LEN + payload.len();
    let mut pkt = vec![0u8; total];

    pkt[0..2].copy_from_slice(&src_port.to_be_bytes());
    pkt[2..4].copy_from_slice(&dst_port.to_be_bytes());
    pkt[4..8].copy_from_slice(&seq.to_be_bytes());
    pkt[8..12].copy_from_slice(&ack.to_be_bytes());
    pkt[12] = 0x80; // Data Offset = 8×4 = 32B
    pkt[13] = 0x18; // PSH + ACK (matching Go)
    pkt[14..16].copy_from_slice(&TCP_WINDOW.to_be_bytes());
    // checksum filled below

    // Options: NOP + NOP + Timestamp(kind=8,len=10,TSval,TSecr)
    pkt[20] = 1;
    pkt[21] = 1;
    pkt[22] = 8;
    pkt[23] = 10;
    pkt[24..28].copy_from_slice(&ts_val.to_be_bytes());
    pkt[28..32].copy_from_slice(&ts_ecr.to_be_bytes());

    pkt[TCP_HDR_LEN..].copy_from_slice(payload);

    let csum = tcp_checksum(src_ip, dst_ip, &pkt);
    pkt[16..18].copy_from_slice(&csum.to_be_bytes());
    pkt
}

// ─── TcpFlowState ────────────────────────────────────────────────────────────

struct TcpFlowState {
    seq: AtomicU32,
    ack: AtomicU32,
    /// Peer timestamp (ts_ecr). Our own ts_val is generated per-send via mono_ms().
    ts_ecr: AtomicU32,
}

// ─── TcpRawConn ──────────────────────────────────────────────────────────────

/// A point-to-point TCP raw connection — acts like a datagram socket to KCP.
pub struct TcpRawConn {
    /// Real TCP socket in TCP_REPAIR mode (held for cleanup).
    _real: std::net::TcpStream,
    /// Raw socket fd shared with capture thread.
    raw_fd: Arc<OwnedFd>,
    /// TCP flow state.
    flow: Arc<TcpFlowState>,
    /// Source/destination for this connection.
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    /// Payload channel from capture thread.
    rx: async_channel::Receiver<Vec<u8>>,
    /// Close signal to capture thread.
    close_tx: async_channel::Sender<()>,
    /// Capture thread handle.
    _cap_thread: Option<thread::JoinHandle<()>>,
}

unsafe impl Send for TcpRawConn {}
unsafe impl Sync for TcpRawConn {}

impl TcpRawConn {
    fn raw_send(&self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > MAX_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("payload too large: {} > {}", buf.len(), MAX_PAYLOAD),
            ));
        }

        let seq = self.flow.seq.load(Ordering::Relaxed);
        let ack = self.flow.ack.load(Ordering::Relaxed);
        // Match Go makeOption: time.Now().UnixNano() / 1e6
        let ts_val = crate::mono_ms() as u32;
        let ts_ecr = self.flow.ts_ecr.load(Ordering::Relaxed);

        let pkt = build_tcp_segment(
            &self.src_ip,
            &self.dst_ip,
            self.src_port,
            self.dst_port,
            seq,
            ack,
            ts_val,
            ts_ecr,
            buf,
        );

        // IPPROTO_TCP raw send: destination is peer IP. Port in sockaddr is
        // ignored by the kernel for raw TCP; TCP header carries real ports.
        let dst = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(self.dst_ip),
            },
            sin_zero: [0; 8],
        };

        let sent = unsafe {
            libc::sendto(
                self.raw_fd.as_raw_fd(),
                pkt.as_ptr() as *const libc::c_void,
                pkt.len(),
                0,
                &dst as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };

        if sent < 0 {
            return Err(io::Error::last_os_error());
        }

        self.flow
            .seq
            .store(seq.wrapping_add(buf.len() as u32), Ordering::Relaxed);

        Ok(buf.len())
    }

    pub fn send_to(&self, buf: &[u8], _target: &SocketAddr) -> io::Result<usize> {
        self.raw_send(buf)
    }

    pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.raw_send(buf)
    }

    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self.rx.try_recv() {
            Ok(payload) => {
                let n = payload.len().min(buf.len());
                buf[..n].copy_from_slice(&payload[..n]);
                let peer = SocketAddr::from((self.dst_ip, self.dst_port));
                Ok((n, peer))
            }
            Err(async_channel::TryRecvError::Empty) => {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(async_channel::TryRecvError::Closed) => {
                Err(io::Error::from(io::ErrorKind::ConnectionReset))
            }
        }
    }

    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self.rx.try_recv() {
            Ok(payload) => {
                let n = payload.len().min(buf.len());
                buf[..n].copy_from_slice(&payload[..n]);
                Ok(n)
            }
            Err(async_channel::TryRecvError::Empty) => {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(async_channel::TryRecvError::Closed) => {
                Err(io::Error::from(io::ErrorKind::ConnectionReset))
            }
        }
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self.rx.recv().await {
            Ok(payload) => {
                let n = payload.len().min(buf.len());
                buf[..n].copy_from_slice(&payload[..n]);
                let peer = SocketAddr::from((self.dst_ip, self.dst_port));
                Ok((n, peer))
            }
            Err(_) => Err(io::Error::from(io::ErrorKind::ConnectionReset)),
        }
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self.rx.recv().await {
            Ok(payload) => {
                let n = payload.len().min(buf.len());
                buf[..n].copy_from_slice(&payload[..n]);
                Ok(n)
            }
            Err(_) => Err(io::Error::from(io::ErrorKind::ConnectionReset)),
        }
    }

    pub async fn send_batch_to<B: AsRef<[u8]>>(
        &self,
        bufs: &[B],
        _target: &SocketAddr,
    ) -> io::Result<()> {
        for buf in bufs {
            self.raw_send(buf.as_ref())?;
        }
        Ok(())
    }

    pub fn try_recv_batch_from(
        &self,
        packet_bufs: &mut [Vec<u8>],
        out: &mut Vec<(Vec<u8>, SocketAddr)>,
    ) -> io::Result<usize> {
        out.clear();
        let peer = SocketAddr::from((self.dst_ip, self.dst_port));
        for slot in packet_bufs.iter_mut() {
            match self.rx.try_recv() {
                Ok(payload) => {
                    *slot = payload.clone();
                    out.push((payload, peer));
                }
                Err(async_channel::TryRecvError::Empty) => break,
                Err(async_channel::TryRecvError::Closed) => {
                    return Err(io::Error::from(io::ErrorKind::ConnectionReset));
                }
            }
        }
        Ok(out.len())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::from((self.src_ip, self.src_port)))
    }
}

impl Drop for TcpRawConn {
    fn drop(&mut self) {
        let _ = self.close_tx.try_send(());
    }
}

// ─── TcpRawListener ──────────────────────────────────────────────────────────

/// Server-side TCP raw listener.
pub struct TcpRawListener {
    real: std::net::TcpListener,
    raw_fd: Arc<OwnedFd>,
    channels: Arc<
        std::sync::Mutex<std::collections::HashMap<SocketAddr, async_channel::Sender<Vec<u8>>>>,
    >,
    /// Per-peer flow state so the capture thread can update ack/seq/ts_ecr.
    flows: Arc<std::sync::Mutex<std::collections::HashMap<SocketAddr, Arc<TcpFlowState>>>>,
    _close_tx: async_channel::Sender<()>,
}

impl TcpRawListener {
    pub fn bind(addr: &SocketAddr) -> io::Result<Self> {
        let real = std::net::TcpListener::bind(*addr)?;
        // Keep the listener **blocking**. `accept()` runs inside `cpu_block`.
        real.set_nonblocking(false)?;

        let raw_fd = Arc::new(open_raw_tcp_socket()?);
        let local_port = real.local_addr()?.port();

        let channels = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let flows = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

        let (close_tx, close_rx) = async_channel::bounded::<()>(1);

        {
            let raw_fd = raw_fd.clone();
            let channels = channels.clone();
            let flows = flows.clone();
            thread::Builder::new()
                .name("tcpraw-srv-capture".into())
                .spawn(move || {
                    server_capture_thread(raw_fd, channels, flows, local_port, close_rx);
                })?;
        }

        Ok(Self {
            real,
            raw_fd,
            channels,
            flows,
            _close_tx: close_tx,
        })
    }

    pub async fn accept(&self) -> io::Result<(TcpRawConn, SocketAddr)> {
        let listener = self.real.try_clone()?;
        let (stream, peer_addr) = crate::task::cpu_block(move || listener.accept()).await?;
        let _ = stream.set_nonblocking(true);

        let local = stream.local_addr()?;
        let (src_ip, src_port) = socket_addr_to_ipv4(&local)?;
        let (dst_ip, dst_port) = socket_addr_to_ipv4(&peer_addr)?;

        // Register rx channel + flow BEFORE TCP_REPAIR so packets that race
        // in during repair setup are not dropped on the floor.
        let (tx, rx) = async_channel::bounded(CHANNEL_CAP);
        let flow = Arc::new(TcpFlowState {
            seq: AtomicU32::new(0),
            ack: AtomicU32::new(0),
            ts_ecr: AtomicU32::new(0),
        });
        {
            let mut channels = self.channels.lock().unwrap();
            channels.insert(peer_addr, tx);
            let mut flows = self.flows.lock().unwrap();
            flows.insert(peer_addr, flow.clone());
        }

        let state = capture_repair_state(&stream)?;
        flow.seq.store(state.seq, Ordering::Relaxed);
        flow.ack.store(state.ack, Ordering::Relaxed);

        let (close_tx, _close_rx) = async_channel::bounded::<()>(1);

        Ok((
            TcpRawConn {
                _real: stream,
                raw_fd: self.raw_fd.clone(),
                flow,
                src_ip,
                dst_ip,
                src_port,
                dst_port,
                rx,
                close_tx,
                _cap_thread: None,
            },
            peer_addr,
        ))
    }
}

impl Drop for TcpRawListener {
    fn drop(&mut self) {
        let _ = self._close_tx.try_send(());
    }
}

// ─── Capture ─────────────────────────────────────────────────────────────────

/// Apply inbound TCP header to flow state (Go captureFlow semantics).
///
/// Caller must ensure `seg` is from the **peer**, not a loopback echo of our
/// own TX (raw IPPROTO_TCP delivers both).
fn update_flow_from_segment(flow: &TcpFlowState, seg: &TcpSegmentView<'_>) {
    // When peer ACKs, our next seq is peer's ACK number (Go: e.seq = tcp.Ack).
    if seg.ack_flag {
        flow.seq.store(seg.ack, Ordering::Relaxed);
    }
    if let Some(ts) = seg.ts_val {
        flow.ts_ecr.store(ts, Ordering::Relaxed);
    }
    // Advance our ACK to cover peer's payload (+ SYN/FIN).
    let mut next = seg.seq.wrapping_add(seg.payload.len() as u32);
    if seg.syn {
        next = next.wrapping_add(1);
    }
    if seg.fin {
        next = next.wrapping_add(1);
    }
    if next != seg.seq {
        // Go: if e.ack == 0 || e.ack == tcp.Seq { e.ack = nextSeq }
        let cur = flow.ack.load(Ordering::Relaxed);
        if cur == 0 || cur == seg.seq {
            flow.ack.store(next, Ordering::Relaxed);
        }
    }
}

fn server_capture_thread(
    raw_fd: Arc<OwnedFd>,
    channels: Arc<
        std::sync::Mutex<std::collections::HashMap<SocketAddr, async_channel::Sender<Vec<u8>>>>,
    >,
    flows: Arc<std::sync::Mutex<std::collections::HashMap<SocketAddr, Arc<TcpFlowState>>>>,
    local_port: u16,
    close_rx: async_channel::Receiver<()>,
) {
    let mut buf = vec![0u8; 65536];
    let fd = raw_fd.as_raw_fd();

    let tv = libc::timeval {
        tv_sec: 1,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }

    loop {
        if close_rx.try_recv().is_ok() {
            return;
        }

        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let n = unsafe {
            libc::recvfrom(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                0,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut addr_len,
            )
        };

        if n <= 0 {
            continue;
        }
        let n = n as usize;
        let Some(seg) = parse_tcp_segment(&buf[..n]) else {
            continue;
        };
        // Port filter (Go captureFlow).
        if seg.dst_port != local_port {
            continue;
        }

        let peer_ip = match sockaddr_to_ipv4(&storage) {
            Some(ip) => ip,
            None => continue,
        };
        let peer = SocketAddr::from((peer_ip, seg.src_port));

        {
            let flows = flows.lock().unwrap();
            if let Some(flow) = flows.get(&peer) {
                update_flow_from_segment(flow, &seg);
            }
        }

        // Go only pushes PSH payloads for data delivery.
        if !seg.psh || seg.payload.is_empty() {
            continue;
        }
        let channels = channels.lock().unwrap();
        if let Some(tx) = channels.get(&peer) {
            let _ = tx.try_send(seg.payload.to_vec());
        }
    }
}

fn client_capture_thread(
    raw_fd: Arc<OwnedFd>,
    tx: async_channel::Sender<Vec<u8>>,
    close_rx: async_channel::Receiver<()>,
    flow: Arc<TcpFlowState>,
    local_port: u16,
    peer_port: u16,
) {
    let mut buf = vec![0u8; 65536];
    let fd = raw_fd.as_raw_fd();

    let tv = libc::timeval {
        tv_sec: 1,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }

    loop {
        if close_rx.try_recv().is_ok() {
            return;
        }

        let n = unsafe {
            libc::recvfrom(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        if n <= 0 {
            continue;
        }
        let n = n as usize;
        let Some(seg) = parse_tcp_segment(&buf[..n]) else {
            continue;
        };
        // Must match our local port AND come from the peer. On loopback,
        // IPPROTO_TCP raw sockets also deliver *our own* TX packets; applying
        // those would corrupt seq/ack and feed our payload back into KCP.
        if seg.dst_port != local_port || seg.src_port != peer_port {
            continue;
        }

        update_flow_from_segment(&flow, &seg);

        if !seg.psh || seg.payload.is_empty() {
            continue;
        }
        let _ = tx.try_send(seg.payload.to_vec());
    }
}

// ─── Packet Parsing ──────────────────────────────────────────────────────────

struct TcpSegmentView<'a> {
    src_port: u16,
    dst_port: u16,
    seq: u32,
    ack: u32,
    ack_flag: bool,
    psh: bool,
    syn: bool,
    fin: bool,
    ts_val: Option<u32>,
    payload: &'a [u8],
}

/// Parse an inbound raw packet.
///
/// Linux `raw(7)`: **receive always includes the IP header**, even for
/// `IPPROTO_TCP` sockets without `IP_HDRINCL`. Send is TCP-segment-only.
fn parse_tcp_segment(buf: &[u8]) -> Option<TcpSegmentView<'_>> {
    if buf.len() < 40 {
        return None;
    }
    // Prefer IP+TCP (normal recv path).
    let (tcp, rest_ok) = if (buf[0] >> 4) == 4 && buf[9] == 6 {
        let ihl = ((buf[0] & 0x0F) as usize) * 4;
        if ihl < 20 || buf.len() < ihl + 20 {
            return None;
        }
        (&buf[ihl..], true)
    } else {
        // Fallback: bare TCP segment (some stacks / loopback quirks).
        (buf, buf.len() >= 20)
    };
    if !rest_ok || tcp.len() < 20 {
        return None;
    }
    let data_off = ((tcp[12] >> 4) as usize) * 4;
    if data_off < 20 || tcp.len() < data_off {
        return None;
    }
    let flags = tcp[13];
    let src_port = u16::from_be_bytes([tcp[0], tcp[1]]);
    let dst_port = u16::from_be_bytes([tcp[2], tcp[3]]);
    let seq = u32::from_be_bytes([tcp[4], tcp[5], tcp[6], tcp[7]]);
    let ack = u32::from_be_bytes([tcp[8], tcp[9], tcp[10], tcp[11]]);
    let ts_val = if data_off > 20 {
        extract_ts_val(&tcp[20..data_off])
    } else {
        None
    };
    Some(TcpSegmentView {
        src_port,
        dst_port,
        seq,
        ack,
        ack_flag: flags & 0x10 != 0,
        psh: flags & 0x08 != 0,
        syn: flags & 0x02 != 0,
        fin: flags & 0x01 != 0,
        ts_val,
        payload: &tcp[data_off..],
    })
}

fn sockaddr_to_ipv4(storage: &libc::sockaddr_storage) -> Option<[u8; 4]> {
    if storage.ss_family as i32 != libc::AF_INET as i32 {
        return None;
    }
    let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
    // s_addr is network-order in memory; copy the four bytes as-is.
    Some(sin.sin_addr.s_addr.to_ne_bytes())
}

fn extract_ts_val(options: &[u8]) -> Option<u32> {
    let mut i = 0;
    while i + 1 < options.len() {
        let kind = options[i];
        if kind <= 1 {
            i += 1;
            continue;
        }
        if i + 1 >= options.len() {
            break;
        }
        let len = options[i + 1] as usize;
        if len < 2 || i + len > options.len() {
            break;
        }
        if kind == 8 && len == 10 {
            return Some(u32::from_be_bytes([
                options[i + 2],
                options[i + 3],
                options[i + 4],
                options[i + 5],
            ]));
        }
        i += len;
    }
    None
}

// ─── Entry Points ────────────────────────────────────────────────────────────

/// Dial a TCP raw connection to `remote_addr` (client side).
pub fn dial(remote_addr: &SocketAddr) -> io::Result<TcpRawConn> {
    let (dst_ip, dst_port) = socket_addr_to_ipv4(remote_addr)?;

    let stream = std::net::TcpStream::connect(*remote_addr)?;
    let local = stream.local_addr()?;
    let (src_ip, src_port) = socket_addr_to_ipv4(&local)?;

    let state = capture_repair_state(&stream)?;
    let raw_fd = Arc::new(open_raw_tcp_socket()?);

    let flow = Arc::new(TcpFlowState {
        seq: AtomicU32::new(state.seq),
        ack: AtomicU32::new(state.ack),
        ts_ecr: AtomicU32::new(0),
    });

    let (tx, rx) = async_channel::bounded(CHANNEL_CAP);
    let (close_tx, close_rx) = async_channel::bounded::<()>(1);

    let cap_handle = {
        let raw_fd = raw_fd.clone();
        let flow = flow.clone();
        thread::Builder::new()
            .name("tcpraw-cli-capture".into())
            .spawn(move || {
                client_capture_thread(raw_fd, tx, close_rx, flow, src_port, dst_port);
            })?
    };

    Ok(TcpRawConn {
        _real: stream,
        raw_fd,
        flow,
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        rx,
        close_tx,
        _cap_thread: Some(cap_handle),
    })
}

/// Create a TCP raw listener bound to `addr` (server side).
pub fn listen(addr: &SocketAddr) -> io::Result<TcpRawListener> {
    TcpRawListener::bind(addr)
}
