//! smol backend: UdpSocket, TcpListener, TcpStream.

use super::{raw_tcp_listener, raw_tcp_stream, raw_udp};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

// ─── UdpSocket ────────────────────────────────────────────────────────────────

pub struct UdpSocket {
    inner: smol::net::UdpSocket,
}

impl UdpSocket {
    /// Create a connected UDP socket (for client use).
    #[inline(always)]
    pub fn connect(bind_addr: SocketAddr, remote_addr: SocketAddr) -> io::Result<Self> {
        let std_sock = raw_udp(bind_addr, Some(remote_addr))?;
        let async_sock = async_io::Async::new(std_sock)?;
        let smol_sock = smol::net::UdpSocket::from(async_sock);
        Ok(Self { inner: smol_sock })
    }

    /// Create a bound UDP socket (for server use).
    #[inline(always)]
    pub fn bind(bind_addr: SocketAddr) -> io::Result<Self> {
        let std_sock = raw_udp(bind_addr, None)?;
        let async_sock = async_io::Async::new(std_sock)?;
        let smol_sock = smol::net::UdpSocket::from(async_sock);
        Ok(Self { inner: smol_sock })
    }

    /// Wrap a pre-configured `std::net::UdpSocket`. The socket must be non-blocking.
    #[inline(always)]
    pub fn from_std(std_sock: std::net::UdpSocket) -> io::Result<Self> {
        let async_sock = async_io::Async::new(std_sock)?;
        let smol_sock = smol::net::UdpSocket::from(async_sock);
        Ok(Self { inner: smol_sock })
    }

    #[inline(always)]
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.recv(buf).await
    }

    #[inline(always)]
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    #[inline(always)]
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.inner.send(buf).await
    }

    #[inline(always)]
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.inner.send_to(buf, target).await
    }

    /// Send all `bufs` without interleaving other work.
    ///
    /// Linux: `sendmmsg` (P1.2b). Other OS: sequential `send` (P1.2a).
    pub async fn send_batch<B: AsRef<[u8]>>(&self, bufs: &[B]) -> io::Result<()> {
        if bufs.is_empty() {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut offset = 0;
            while offset < bufs.len() {
                match super::mmsg::sendmmsg_connected(self.inner.as_raw_fd(), &bufs[offset..]) {
                    Ok(n) if n > 0 => offset += n,
                    Ok(_) => {
                        // Wait until writable via a no-op send readiness: poll send of empty fails,
                        // so use async send of first remaining packet as readiness probe.
                        let _ = self.inner.send(bufs[offset].as_ref()).await?;
                        offset += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        let _ = self.inner.send(bufs[offset].as_ref()).await?;
                        offset += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        {
            for buf in bufs {
                let mut remaining = buf.as_ref();
                while !remaining.is_empty() {
                    let n = self.inner.send(remaining).await?;
                    remaining = &remaining[n..];
                }
            }
            Ok(())
        }
    }

    /// Send all `bufs` to `target`.
    pub async fn send_batch_to<B: AsRef<[u8]>>(
        &self,
        bufs: &[B],
        target: SocketAddr,
    ) -> io::Result<()> {
        if bufs.is_empty() {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut offset = 0;
            while offset < bufs.len() {
                match super::mmsg::sendmmsg_to(self.inner.as_raw_fd(), &bufs[offset..], &target) {
                    Ok(n) if n > 0 => offset += n,
                    Ok(_) => {
                        let _ = self.inner.send_to(bufs[offset].as_ref(), target).await?;
                        offset += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        let _ = self.inner.send_to(bufs[offset].as_ref(), target).await?;
                        offset += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        {
            for buf in bufs {
                let mut remaining = buf.as_ref();
                while !remaining.is_empty() {
                    let n = self.inner.send_to(remaining, target).await?;
                    remaining = &remaining[n..];
                }
            }
            Ok(())
        }
    }

    /// Non-blocking recv for connected sockets.
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        // async-net has no try_recv API. Use the socket directly on platforms
        // that provide MSG_DONTWAIT so callers can drain a UDP burst without
        // waiting for one reactor wake per datagram.
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        ))]
        {
            use std::os::fd::AsRawFd;
            let fd = self.inner.as_raw_fd();
            let n = unsafe {
                libc::recv(
                    fd,
                    buf.as_mut_ptr() as *mut _,
                    buf.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(n as usize)
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        )))]
        {
            let _ = buf;
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
    }

    /// Non-blocking send for connected sockets (libc `send` + MSG_DONTWAIT).
    /// All BSD-derived systems (macOS, FreeBSD, OpenBSD, NetBSD, DragonFly) and
    /// Linux support MSG_DONTWAIT; other platforms fall back to WouldBlock to
    /// force the async path.
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        ))]
        {
            use std::os::fd::AsRawFd;
            let fd = self.inner.as_raw_fd();
            let n =
                unsafe { libc::send(fd, buf.as_ptr() as *const _, buf.len(), libc::MSG_DONTWAIT) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(n as usize)
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        )))]
        {
            let _ = buf;
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
    }

    /// Try to send all `bufs` without blocking. Returns the number of packets
    /// sent (stops on `WouldBlock` / partial).
    ///
    /// Linux: `sendmmsg` (one syscall for the batch). Others: per-packet
    /// [`try_send`](Self::try_send).
    pub fn try_send_batch<B: AsRef<[u8]>>(&self, bufs: &[B]) -> io::Result<usize> {
        if bufs.is_empty() {
            return Ok(0);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            match super::mmsg::sendmmsg_connected(self.inner.as_raw_fd(), bufs) {
                Ok(n) => Ok(n),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
                Err(e) => Err(e),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut sent = 0;
            for b in bufs {
                match self.try_send(b.as_ref()) {
                    Ok(n) if n == b.as_ref().len() => sent += 1,
                    Ok(_) => return Ok(sent), // partial datagram (shouldn't happen)
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(sent),
                    Err(e) => return Err(e),
                }
            }
            Ok(sent)
        }
    }

    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        ))]
        {
            use std::os::fd::AsRawFd;
            let fd = self.inner.as_raw_fd();
            let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            let n = unsafe {
                libc::recvfrom(
                    fd,
                    buf.as_mut_ptr() as *mut _,
                    buf.len(),
                    libc::MSG_DONTWAIT,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut len,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: recvfrom initialized `storage` and wrote its actual byte
            // length to `len`; SockAddr owns the copied storage value.
            let peer = unsafe { socket2::SockAddr::new(storage, len) }
                .as_socket()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown family"))?;
            Ok((n as usize, peer))
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
        )))]
        {
            let _ = buf;
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
    }

    /// Drain ready datagrams (Linux: recvmmsg; else sequential try_recv_from).
    pub fn try_recv_batch_from(
        &self,
        packet_bufs: &mut [Vec<u8>],
        out: &mut Vec<(Vec<u8>, SocketAddr)>,
    ) -> io::Result<usize> {
        out.clear();
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            match super::mmsg::recvmmsg_from(self.inner.as_raw_fd(), packet_bufs) {
                Ok(msgs) => {
                    for (i, (n, addr)) in msgs.into_iter().enumerate() {
                        if let Some(peer) = addr {
                            let mut v = std::mem::take(&mut packet_bufs[i]);
                            v.truncate(n);
                            // Replace slot with a fresh empty buffer for next batch.
                            packet_bufs[i] = Vec::with_capacity(v.capacity().max(2048));
                            out.push((v, peer));
                        }
                    }
                    return Ok(out.len());
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(0),
                Err(e) => return Err(e),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            for slot in packet_bufs.iter_mut() {
                if slot.capacity() < 2048 {
                    slot.reserve(2048);
                }
                slot.resize(slot.capacity(), 0);
                match self.try_recv_from(slot) {
                    Ok((n, peer)) => {
                        let payload = slot[..n].to_vec();
                        slot.clear();
                        out.push((payload, peer));
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
            Ok(out.len())
        }
    }

    /// Allocation-free batch receive on an unconnected socket.
    ///
    /// Fills `packet_bufs[..n]` with payloads **in place** (slots stay owned by
    /// the caller, no per-slot replacement `Vec`) and writes the source
    /// addresses into `peers` (cleared first; capacity reused). Returns the
    /// number of datagrams received; `Ok(0)` on `WouldBlock`.
    ///
    /// Linux: one `recvmmsg` syscall. Elsewhere: sequential `try_recv_from`
    /// filling slots in place (no `to_vec()` copy).
    pub fn try_recv_batch_from_into(
        &self,
        packet_bufs: &mut [Vec<u8>],
        peers: &mut Vec<SocketAddr>,
    ) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            return super::mmsg::recvmmsg_from_into(self.inner.as_raw_fd(), packet_bufs, peers);
        }
        #[cfg(not(target_os = "linux"))]
        {
            peers.clear();
            let mut n = 0;
            for slot in packet_bufs.iter_mut() {
                if slot.capacity() < 2048 {
                    slot.reserve(2048);
                }
                slot.resize(slot.capacity(), 0);
                match self.try_recv_from(slot) {
                    Ok((len, peer)) => {
                        slot.truncate(len);
                        peers.push(peer);
                        n += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
            Ok(n)
        }
    }

    #[inline(always)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Non-blocking batch receive on a connected socket (no peer addresses).
    ///
    /// Linux/macOS: one `recvmmsg` syscall fills the pool. Others: one
    /// `try_recv` per call into `pool[0]`. Returns datagrams received.
    pub fn try_recv_batch(&self, pool: &mut [Vec<u8>]) -> io::Result<usize> {
        if pool.is_empty() {
            return Ok(0);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            match super::mmsg::recvmmsg_connected(self.inner.as_raw_fd(), pool) {
                Ok(n) => Ok(n),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
                Err(e) => Err(e),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            if pool[0].capacity() < 2048 {
                pool[0].reserve(2048);
            }
            // resize to capacity so try_recv gets a non-zero-length buffer.
            pool[0].resize(pool[0].capacity(), 0);
            match self.try_recv(&mut pool[0]) {
                Ok(n) if n > 0 => {
                    pool[0].truncate(n);
                    Ok(1)
                }
                Ok(_) => Ok(0),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
                Err(e) => Err(e),
            }
        }
    }
}

// ─── TcpListener ──────────────────────────────────────────────────────────────

pub struct TcpListener {
    inner: smol::net::TcpListener,
}

impl TcpListener {
    #[inline(always)]
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let std_listener = raw_tcp_listener(addr)?;
        let async_listener = async_io::Async::new(std_listener)?;
        let l = smol::net::TcpListener::from(async_listener);
        Ok(Self { inner: l })
    }

    #[inline(always)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    #[inline(always)]
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (s, a) = self.inner.accept().await?;
        // Match Go net.TCPConn defaults and raw_tcp_stream: disable Nagle.
        let _ = s.set_nodelay(true);
        Ok((TcpStream { inner: s }, a))
    }

    /// Non-blocking accept of one pending connection; `WouldBlock` when none.
    ///
    /// Polls the async `accept` future once with a no-op waker.
    #[inline(always)]
    pub fn try_accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = pin!(self.inner.accept());
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok((s, a))) => {
                let _ = s.set_nodelay(true);
                Ok((TcpStream { inner: s }, a))
            }
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "no pending connection",
            )),
        }
    }
}

// ─── TcpStream ────────────────────────────────────────────────────────────────

pub struct TcpStream {
    inner: smol::net::TcpStream,
}

impl TcpStream {
    #[inline(always)]
    pub async fn connect(addr: impl AsRef<str>) -> io::Result<Self> {
        let addr = addr.as_ref().to_owned();
        // Offload blocking DNS resolution + TCP connect to the persistent
        // blocking pool. Try all resolved addresses (IPv6/IPv4 fallback).
        let std_stream = crate::cpu_block(move || -> io::Result<std::net::TcpStream> {
            let addrs = addr
                .to_socket_addrs()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let mut last_err = None;
            for remote in addrs {
                match raw_tcp_stream(remote) {
                    Ok(s) => return Ok(s),
                    Err(e) => last_err = Some(e),
                }
            }
            Err(last_err.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("no address for {addr}"),
                )
            }))
        })
        .await;
        let async_stream = async_io::Async::new(std_stream?)?;
        let s = smol::net::TcpStream::from(async_stream);
        Ok(Self { inner: s })
    }
}

impl crate::AsyncRead for TcpStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl crate::AsyncWrite for TcpStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}
