//! Datagram transport layer for the async `KcpConn`.
//!
//! [`PacketTransport`] is the pluggable packet-delivery abstraction under
//! [`crate::KcpConn`]; [`PeerQueue`]/[`PeerTransport`] back the shared-socket
//! server demultiplexer in [`crate::KcpListener`] (per-peer inbound queues and
//! the per-peer transport fed from them).

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

/// Max UDP datagram size for the input-loop recv buffers and peer queues.
pub(crate) const MAX_DATAGRAM: usize = 2048;
/// Bound retained per-peer receive storage after a transient queue burst.
pub(crate) const MAX_RETAINED_PEER_BUFFERS: usize = 64;

// ─── PacketTransport ──────────────────────────────────────────────────────────

/// Pluggable datagram layer under [`crate::KcpConn`].
///
/// Implementations: [`kio::DatagramSocket`] (plain UDP / TcpRaw) and
/// `kcptun_common::CryptoTransport` (encrypt/decrypt wrapper).
///
/// Uses `#[async_trait]` so async methods are object-safe without hand-written
/// future return types.  Each implementation saves ~15 lines of
/// `Box::pin(async move { ... })` boilerplate.
#[async_trait::async_trait]
pub trait PacketTransport: Send + Sync {
    /// Read one datagram into `buf`. Returns bytes written.
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Non-blocking read; `WouldBlock` when nothing ready.
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Read one datagram into reusable owned storage.
    ///
    /// The default delegates to [`recv`](Self::recv). Queue-backed transports
    /// may override this to transfer packet ownership without another copy.
    async fn recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let n = self.recv(buf.as_mut_slice()).await?;
        buf.truncate(n);
        Ok(n)
    }

    /// Non-blocking counterpart to [`recv_vec`](Self::recv_vec).
    fn try_recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let n = self.try_recv(buf.as_mut_slice())?;
        buf.truncate(n);
        Ok(n)
    }

    /// Batch-send on a connected socket.
    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()>;

    /// Batch-send to an explicit peer (unconnected socket).
    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()>;

    /// High-priority send (ACK path). Default = [`send_batch`](Self::send_batch).
    /// Crypto wrappers use a separate buffer here to avoid lock contention.
    async fn send_urgent(&self, packets: &[Bytes]) -> io::Result<()> {
        self.send_batch(packets).await
    }

    /// High-priority send_to (ACK path, unconnected). Default = send_batch_to.
    async fn send_urgent_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        self.send_batch_to(packets, target).await
    }

    /// Non-blocking batch send (connected socket). Returns the number of
    /// datagrams handed to the kernel, stopping at the first `WouldBlock`
    /// (socket send buffer full); the caller must re-queue `packets[sent..]`
    /// for a later send (e.g. via the flush loop).
    ///
    /// Default: unavailable → `Err(WouldBlock)`, so callers fall back to the
    /// async flush-loop path (existing behavior). `kio::DatagramSocket`
    /// overrides this with a real non-blocking send.
    fn try_send_batch(&self, _packets: &[Bytes]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    /// Non-blocking batch receive into the caller's buffer pool. Returns the
    /// number of datagrams received. Default: one via [`try_recv`](Self::try_recv)
    /// into `pool[0]`.
    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        if pool.is_empty() {
            return Ok(0);
        }
        match self.try_recv(&mut pool[0]) {
            Ok(n) if n > 0 => {
                pool[0].truncate(n);
                Ok(1)
            }
            Ok(_) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Whether [`try_recv_batch`](Self::try_recv_batch) can receive multiple
    /// datagrams per call (vs. the default single). The input loop uses this to
    /// switch to the batch drain (recvmmsg on Linux).
    fn supports_recv_batch(&self) -> bool {
        false
    }

    fn local_addr(&self) -> io::Result<SocketAddr>;
}

#[async_trait::async_trait]
impl PacketTransport for kio::DatagramSocket {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        // Call inherent method (not trait) to avoid recursion.
        kio::DatagramSocket::recv(self, buf).await
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        kio::DatagramSocket::try_recv(self, buf)
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        kio::DatagramSocket::send_batch(self, packets).await
    }

    async fn send_batch_to(&self, packets: &[Bytes], target: SocketAddr) -> io::Result<()> {
        kio::DatagramSocket::send_batch_to(self, packets, target).await
    }

    fn try_send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        kio::DatagramSocket::try_send_batch(self, packets)
    }

    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        kio::DatagramSocket::try_recv_batch(self, pool)
    }

    fn supports_recv_batch(&self) -> bool {
        true
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        kio::DatagramSocket::local_addr(self)
    }
}

// ─── Per-peer queue + transport (KcpListener demux) ───────────────────────────

/// Per-peer inbound queue + wakeup used by [`crate::KcpListener`] to feed one
/// shared bound socket's datagrams into each accepted [`crate::KcpConn`].
///
/// The queue is **drop-tail bounded**: when `max_packets` is reached, new
/// datagrams are dropped (the listener must never wait on a peer's queue, or a
/// slow peer would stall the whole shared socket). KCP retransmission recovers
/// dropped datagrams.
pub(crate) struct PeerQueue {
    buffers: Mutex<PeerBuffers>,
    notify: kio::Notify,
    closed: AtomicBool,
    /// Drop-tail cap on queued packets (0 = unbounded).
    max_packets: usize,
    /// Shared listener drop counter (relaxed increments; opt-in observability).
    drops: Arc<AtomicU64>,
}

struct PeerBuffers {
    packets: VecDeque<Vec<u8>>,
    packet_bytes: usize,
    spare: Vec<Vec<u8>>,
}

impl PeerQueue {
    pub(crate) fn new(max_packets: usize, drops: Arc<AtomicU64>) -> Self {
        // Keep only a tiny spare-vector index up front. Packet-sized buffers
        // are allocated lazily as traffic arrives, avoiding 128KiB of eager
        // storage for every idle peer; the recycle cap still bounds retained
        // memory after bursts.
        let spare = Vec::with_capacity(2);
        Self {
            buffers: Mutex::new(PeerBuffers {
                packets: VecDeque::new(),
                packet_bytes: 0,
                spare,
            }),
            notify: kio::Notify::new(),
            closed: AtomicBool::new(false),
            max_packets,
            drops,
        }
    }

    /// Queue a packet and return storage suitable for the listener's next recv,
    /// plus whether the packet actually entered the queue. On drop-tail the
    /// dropped packet's buffer is recycled as the returned recv slot (no fresh
    /// alloc) and `queued` is `false` so the reader skips a pointless notify.
    pub(crate) fn push_and_reuse(&self, pkt: Vec<u8>) -> (Vec<u8>, bool) {
        let mut buffers = self.buffers.lock();
        if self.max_packets > 0 && buffers.packets.len() >= self.max_packets {
            // Drop-tail: drop the newest datagram (keep queued order intact),
            // recycling its buffer as the next recv slot — no fresh alloc.
            self.drops.fetch_add(1, Ordering::Relaxed);
            let mut next = pkt;
            next.resize(MAX_DATAGRAM, 0);
            (next, false)
        } else {
            buffers.packet_bytes += pkt.len();
            buffers.packets.push_back(pkt);
            (
                buffers
                    .spare
                    .pop()
                    .unwrap_or_else(|| vec![0u8; MAX_DATAGRAM]),
                true,
            )
        }
    }

    fn pop(&self) -> Option<Vec<u8>> {
        let mut buffers = self.buffers.lock();
        let pkt = buffers.packets.pop_front()?;
        buffers.packet_bytes = buffers.packet_bytes.saturating_sub(pkt.len());
        Some(pkt)
    }

    /// Move a queued datagram into the consumer buffer and recycle the
    /// consumer's previous allocation back to the listener.
    fn pop_into(&self, buf: &mut Vec<u8>) -> Option<usize> {
        let mut buffers = self.buffers.lock();
        let mut pkt = buffers.packets.pop_front()?;
        buffers.packet_bytes = buffers.packet_bytes.saturating_sub(pkt.len());
        std::mem::swap(buf, &mut pkt);
        let n = buf.len();
        if buffers.spare.len() < MAX_RETAINED_PEER_BUFFERS {
            pkt.resize(MAX_DATAGRAM, 0);
            buffers.spare.push(pkt);
        }
        Some(n)
    }

    /// Pop up to `pool.len()` queued datagrams under **one lock**, swapping each
    /// consumer buffer into the queue's spare pool (recycles capacity). Returns
    /// the number popped; `WouldBlock` when the queue is empty. Keeps queued
    /// order, so a peer's input loop drains a whole burst and batches its ACKs.
    fn pop_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        let mut buffers = self.buffers.lock();
        let mut n = 0;
        while n < pool.len() {
            let mut pkt = match buffers.packets.pop_front() {
                Some(p) => p,
                None => break,
            };
            buffers.packet_bytes = buffers.packet_bytes.saturating_sub(pkt.len());
            std::mem::swap(&mut pool[n], &mut pkt);
            if buffers.spare.len() < MAX_RETAINED_PEER_BUFFERS {
                pkt.resize(MAX_DATAGRAM, 0);
                buffers.spare.push(pkt);
            }
            n += 1;
        }
        if n == 0 {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        } else {
            Ok(n)
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wake a single waiting consumer. The listener routes a whole socket drain
    /// into peer queues, then notifies each affected queue once so the peer
    /// input loop drains the burst and batches ACKs.
    pub(crate) fn notify_one(&self) {
        self.notify.notify_one();
    }
}

/// `PacketTransport` for one accepted peer: reads inbound from its
/// [`PeerQueue`] and writes outbound on the shared listen socket addressed to
/// that peer.
///
/// Dropping the transport (i.e. dropping the accepted `KcpConn`) closes the
/// peer queue so the listener reaps it and can accept a fresh connection from
/// the same address.
pub(crate) struct PeerTransport {
    pub(crate) queue: Arc<PeerQueue>,
    pub(crate) socket: Arc<kio::DatagramSocket>,
    pub(crate) peer: SocketAddr,
}

#[async_trait::async_trait]
impl PacketTransport for PeerTransport {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
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

    async fn recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        loop {
            if let Some(n) = self.queue.pop_into(buf) {
                return Ok(n);
            }
            if self.queue.is_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "KcpConn: peer session closed",
                ));
            }
            let notified = self.queue.notify.notified();
            if let Some(n) = self.queue.pop_into(buf) {
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
    }

    fn try_recv_vec(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.queue
            .pop_into(buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "peer queue empty"))
    }

    fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        self.queue.pop_batch(pool)
    }

    fn supports_recv_batch(&self) -> bool {
        true
    }

    async fn send_batch(&self, packets: &[Bytes]) -> io::Result<()> {
        self.socket.send_batch_to(packets, self.peer).await
    }

    async fn send_batch_to(&self, packets: &[Bytes], _target: SocketAddr) -> io::Result<()> {
        self.socket.send_batch_to(packets, self.peer).await
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

/// Optional per-accepted-peer transport wrapper applied by
/// [`crate::KcpListenerBuilder`] (e.g. adding encryption while retaining the
/// listener's single shared-socket reader).
pub(crate) type TransportWrapper =
    Arc<dyn Fn(Arc<dyn PacketTransport>, SocketAddr) -> Arc<dyn PacketTransport> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Batch pop preserves queue order and recycles the consumer buffers.
    #[test]
    fn pop_batch_preserves_order_and_recycles() {
        let drops = Arc::new(AtomicU64::new(0));
        let q = PeerQueue::new(8, drops);
        for i in 0..5 {
            q.push_and_reuse(vec![0u8; 10 + i]); // payload sizes 10..14
        }
        let mut pool: Vec<Vec<u8>> = (0..3).map(|_| vec![0u8; MAX_DATAGRAM]).collect();
        let n = q.pop_batch(&mut pool).unwrap();
        assert_eq!(n, 3);
        assert_eq!(pool[0].len(), 10);
        assert_eq!(pool[1].len(), 11);
        assert_eq!(pool[2].len(), 12);

        let mut pool2: Vec<Vec<u8>> = (0..3).map(|_| vec![0u8; MAX_DATAGRAM]).collect();
        let n2 = q.pop_batch(&mut pool2).unwrap();
        assert_eq!(n2, 2);
        assert_eq!(pool2[0].len(), 13);
        assert_eq!(pool2[1].len(), 14);

        let err = q.pop_batch(&mut pool2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    /// Drop-tail cap keeps the first queued packets and drops new ones once the
    /// bound is reached; the drop counter reflects the tail drops.
    #[test]
    fn push_drop_tail_when_bounded() {
        let drops = Arc::new(AtomicU64::new(0));
        let q = PeerQueue::new(3, drops.clone());
        for i in 0..5 {
            q.push_and_reuse(vec![0u8; i + 1]);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        let mut pool: Vec<Vec<u8>> = (0..3).map(|_| vec![0u8; MAX_DATAGRAM]).collect();
        let n = q.pop_batch(&mut pool).unwrap();
        assert_eq!(n, 3);
        // First three enqueued survive (sizes 1,2,3); the last two are dropped.
        assert_eq!(pool[0].len(), 1);
        assert_eq!(pool[1].len(), 2);
        assert_eq!(pool[2].len(), 3);
    }

    /// Drop-tail recycles the dropped packet's buffer as the next recv slot
    /// (capacity preserved, no fresh allocation) and reports `queued = false`.
    #[test]
    fn push_drop_tail_recycles_buffer() {
        let drops = Arc::new(AtomicU64::new(0));
        let q = PeerQueue::new(1, drops.clone());
        // First datagram queues (queue cap = 1).
        let (_, queued) = q.push_and_reuse(vec![1u8; 5]);
        assert!(queued);
        // Second datagram is drop-tailed; its buffer is recycled in place.
        let mut dropped = vec![2u8; 3];
        dropped.reserve(MAX_DATAGRAM);
        let (recycled, queued) = q.push_and_reuse(dropped);
        assert!(!queued);
        assert_eq!(recycled.len(), MAX_DATAGRAM);
        assert!(recycled.capacity() >= MAX_DATAGRAM);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn peer_queue_spares_are_lazy() {
        let drops = Arc::new(AtomicU64::new(0));
        let q = PeerQueue::new(0, drops);
        let buffers = q.buffers.lock();
        assert!(
            buffers.spare.is_empty(),
            "idle peers must not preallocate packet buffers"
        );
    }
}
