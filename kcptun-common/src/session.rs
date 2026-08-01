//! Encrypted KCP session helpers (`CryptoTransport` + `kcp_session`).
//!
//! Protocol stack (matches Go kcp-go v5 / kcptun):
//!
//! ```text
//! inbound:  UDP → decrypt → FEC → KCP
//! outbound: KCP → FEC → encrypt → UDP
//! ```
//!
//! Encryption lives **outside** [`kcp_rs::KcpConn`]: this module wraps a plain
//! datagram socket and implements [`kcp_rs::PacketTransport`] so KcpConn talks
//! to ciphertext transparently.
//!
//! Feature-gated on `tokio` / `smol` (needs kio + kcp-rs async).

use std::future::Future;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use kcp_rs::{KcpConfig, KcpConn, PacketTransport};
use kcrypt_rs::crypt::CryptEngine;
use kcrypt_rs::wire::{
    decrypt_cfb_in_place, encrypt_batch, encrypt_batch_into, inbound_null,
    should_cpu_block_encrypt, CryptoBuf,
};

/// Default conversation ID used as CryptoBuf session_id seed (matches client).
const DEFAULT_CONV: u32 = 0xDEAD_BEEF;
/// Distinct seed for ACK-path CryptoBuf so nonces never collide with data path.
const ACK_SESSION_XOR: u64 = 0xA11C_B0FF_u64;

// ─── CryptoTransport ──────────────────────────────────────────────────────────

/// Datagram transport that encrypts/decrypts packets around an inner socket.
///
/// - `"null"`: passthrough (no header)
/// - CFB ciphers / `"none"`: 20B header `[nonce 16][CRC32 4][payload]`
/// - `"aes-128-gcm"`: AEAD seal/open
///
/// Data path uses `data_crypto_buf`; ACK / urgent path uses `ack_crypto_buf`
/// (matches client `ack_crypto_buf` intent — avoid lock contention with flush).
pub struct CryptoTransport {
    inner: Arc<kio::DatagramSocket>,
    crypt: Arc<CryptEngine>,
    /// `crypt != "null"` — `"none"` still packs a CFB header.
    has_encryption: bool,
    data_crypto_buf: Arc<Mutex<CryptoBuf>>,
    ack_crypto_buf: Arc<Mutex<CryptoBuf>>,
}

impl CryptoTransport {
    /// Wrap `inner` with the given cipher method and raw 32-byte key material.
    ///
    /// `method` is the Go-compatible name (`"aes"`, `"null"`, `"aes-128-gcm"`, …).
    pub fn new(inner: Arc<kio::DatagramSocket>, key: &[u8], method: &str) -> Self {
        let (engine, _) = CryptEngine::select(method, key);
        Self::from_engine(inner, Arc::new(engine), method != "null")
    }

    /// Wrap with a pre-built [`CryptEngine`].
    pub fn from_engine(
        inner: Arc<kio::DatagramSocket>,
        crypt: Arc<CryptEngine>,
        has_encryption: bool,
    ) -> Self {
        Self {
            inner,
            crypt,
            has_encryption,
            data_crypto_buf: Arc::new(Mutex::new(CryptoBuf::new(DEFAULT_CONV as u64))),
            ack_crypto_buf: Arc::new(Mutex::new(CryptoBuf::new(
                (DEFAULT_CONV as u64) ^ ACK_SESSION_XOR,
            ))),
        }
    }

    /// Access the underlying socket (diagnostics / local_addr).
    pub fn inner(&self) -> &Arc<kio::DatagramSocket> {
        &self.inner
    }

    pub fn crypt(&self) -> &Arc<CryptEngine> {
        &self.crypt
    }

    pub fn has_encryption(&self) -> bool {
        self.has_encryption
    }

    /// Encrypt a batch on the **data** path (flush loop). May offload heavy
    /// cipher batches to `cpu_block` (M0.2).
    async fn encrypt_data(&self, packets: Vec<Bytes>) -> Vec<Bytes> {
        self.encrypt_with(&self.data_crypto_buf, packets, false)
            .await
    }

    /// Encrypt a batch on the **ACK / urgent** path. **Never offloads** to
    /// `cpu_block`: ACKs must go out promptly or the peer's send window stalls
    /// (with FEC, a single ACK expands to a 13-packet batch that would trip the
    /// offload heuristic and add a blocking-pool hop). Matches the legacy
    /// binary's inline ACK encrypt.
    async fn encrypt_urgent(&self, packets: Vec<Bytes>) -> Vec<Bytes> {
        self.encrypt_with(&self.ack_crypto_buf, packets, true).await
    }

    async fn encrypt_with(
        &self,
        crypto_buf: &Arc<Mutex<CryptoBuf>>,
        packets: Vec<Bytes>,
        force_inline: bool,
    ) -> Vec<Bytes> {
        if packets.is_empty() {
            return packets;
        }
        let has_aead = self.crypt.is_aead();
        let total_bytes: usize = packets.iter().map(|p| p.len()).sum();
        let use_cpu_block = !force_inline
            && should_cpu_block_encrypt(
                self.has_encryption,
                has_aead,
                packets.len(),
                total_bytes,
                self.crypt.as_ref(),
            );
        let allow_parallel = !use_cpu_block;
        if use_cpu_block {
            // Heavy cipher + large batch: offload to the blocking pool so the
            // reactor can keep draining UDP / processing ACKs (matches the
            // legacy binary flush path; M0.2 — was previously done inline).
            let crypt = self.crypt.clone();
            let cb = crypto_buf.clone();
            let has_encryption = self.has_encryption;
            kio::cpu_block(move || {
                encrypt_batch(packets, crypt.as_ref(), &cb, has_encryption, allow_parallel)
            })
            .await
        } else {
            let mut out = Vec::with_capacity(packets.len());
            encrypt_batch_into(
                packets,
                self.crypt.as_ref(),
                crypto_buf,
                self.has_encryption,
                allow_parallel,
                &mut out,
            );
            out
        }
    }

    /// Decrypt one inbound datagram in-place; returns plaintext length in `buf`.
    ///
    /// Failed decrypt returns `Ok(0)` so the input loop can skip (matches client
    /// which drains next datagram rather than killing the session on CRC fail).
    fn decrypt_in_place(&self, buf: &mut [u8], n: usize) -> io::Result<usize> {
        if n == 0 {
            return Ok(0);
        }
        if self.crypt.is_aead() {
            let aead = self.crypt.as_aead().expect("is_aead");
            match aead.open(&buf[..n]) {
                Ok(plain) => {
                    let len = plain.len();
                    if len > buf.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "AEAD plaintext longer than recv buffer",
                        ));
                    }
                    buf[..len].copy_from_slice(&plain);
                    Ok(len)
                }
                Err(_) => {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                    Ok(0)
                }
            }
        } else if self.has_encryption {
            match decrypt_cfb_in_place(&mut buf[..n], self.crypt.as_ref(), false) {
                Ok(body) => {
                    let len = body.len();
                    // body is a subslice of buf; shift to front if needed.
                    let off = n - len;
                    if off > 0 {
                        buf.copy_within(off..n, 0);
                    }
                    Ok(len)
                }
                Err(_) => {
                    kcp_rs::snmp_add(&kcp_rs::DEFAULT_SNMP.in_csum_errors, 1);
                    Ok(0)
                }
            }
        } else {
            // null: identity
            let _ = inbound_null(&buf[..n]);
            Ok(n)
        }
    }
}

impl PacketTransport for CryptoTransport {
    fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            // Skip bad packets until one decrypts or the socket errors.
            loop {
                let n = kio::DatagramSocket::recv(self.inner.as_ref(), buf).await?;
                let plain = self.decrypt_in_place(buf, n)?;
                if plain > 0 || n == 0 {
                    return Ok(plain);
                }
                // CRC/AEAD fail → try next datagram (non-blocking drain first).
                match kio::DatagramSocket::try_recv(self.inner.as_ref(), buf) {
                    Ok(m) => {
                        let plain = self.decrypt_in_place(buf, m)?;
                        if plain > 0 || m == 0 {
                            return Ok(plain);
                        }
                        continue;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // Fall through to another blocking recv.
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }

    fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = kio::DatagramSocket::try_recv(self.inner.as_ref(), buf)?;
            let plain = self.decrypt_in_place(buf, n)?;
            if plain > 0 || n == 0 {
                return Ok(plain);
            }
            // bad packet — try another if ready
        }
    }

    fn send_batch<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if packets.is_empty() {
                return Ok(());
            }
            let encrypted = self.encrypt_data(packets.to_vec()).await;
            kio::DatagramSocket::send_batch(self.inner.as_ref(), &encrypted).await
        })
    }

    fn send_batch_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if packets.is_empty() {
                return Ok(());
            }
            let encrypted = self.encrypt_data(packets.to_vec()).await;
            kio::DatagramSocket::send_batch_to(self.inner.as_ref(), &encrypted, target).await
        })
    }

    fn send_urgent<'a>(
        &'a self,
        packets: &'a [Bytes],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if packets.is_empty() {
                return Ok(());
            }
            let encrypted = self.encrypt_urgent(packets.to_vec()).await;
            kio::DatagramSocket::send_batch(self.inner.as_ref(), &encrypted).await
        })
    }

    fn send_urgent_to<'a>(
        &'a self,
        packets: &'a [Bytes],
        target: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if packets.is_empty() {
                return Ok(());
            }
            let encrypted = self.encrypt_urgent(packets.to_vec()).await;
            kio::DatagramSocket::send_batch_to(self.inner.as_ref(), &encrypted, target).await
        })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        kio::DatagramSocket::local_addr(self.inner.as_ref())
    }
}

// ─── kcp_session factory ──────────────────────────────────────────────────────

/// Dial `addr` over UDP with encryption + optional FEC, returning a [`KcpConn`].
///
/// ```no_run
/// use kcptun_common::{derive_key, kcp_session};
/// use kcp_rs::KcpConfig;
/// # fn main() {
/// # let _fut = async {
/// let key = derive_key("secret");
/// let conn = kcp_session("127.0.0.1:29900", &key, "aes", KcpConfig::default()).await?;
/// # Ok::<_, std::io::Error>(conn)
/// # };
/// # }
/// ```
///
/// Stack built: `UDP → CryptoTransport → KcpConn(.fec)` — crypto wraps whole
/// FEC frames (offset 0), matching Go session layout.
pub async fn kcp_session(
    addr: impl ToSocketAddrs,
    key: &[u8],
    crypt: &str,
    config: KcpConfig,
) -> io::Result<KcpConn> {
    let remote = resolve_one(addr)?;
    let bind = if remote.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0u16; 8], 0))
    };
    let udp = kio::UdpSocket::connect(bind, remote)?;
    let inner = Arc::new(kio::DatagramSocket::Udp(udp));
    let ct = CryptoTransport::new(inner, key, crypt);
    let transport: Arc<dyn PacketTransport> = Arc::new(ct);

    let mut builder = KcpConn::with_transport(transport, remote)
        .connected(true)
        .config(config.clone());
    if config.datashard > 0 && config.parityshard > 0 {
        builder = builder.fec(config.datashard, config.parityshard);
    }
    builder.build().await
}

/// Like [`kcp_session`] but wraps an existing connected [`kio::DatagramSocket`].
pub async fn kcp_session_with_socket(
    socket: Arc<kio::DatagramSocket>,
    remote: SocketAddr,
    key: &[u8],
    crypt: &str,
    config: KcpConfig,
    connected: bool,
) -> io::Result<KcpConn> {
    let ct = CryptoTransport::new(socket, key, crypt);
    let transport: Arc<dyn PacketTransport> = Arc::new(ct);
    let mut builder = KcpConn::with_transport(transport, remote)
        .connected(connected)
        .config(config.clone());
    if config.datashard > 0 && config.parityshard > 0 {
        builder = builder.fec(config.datashard, config.parityshard);
    }
    builder.build().await
}

// ─── Client / server-shaped helpers (Task 4 incremental) ──────────────────────
//
// Production binaries still own the custom KCP+SMUX+Snappy flush loops.
// These helpers build a bare [`KcpConn`] (crypto+FEC+KCP only) for tests and
// for a future cut-over that keeps Snappy/SMUX outside KcpConn:
//
//   SMUX prepare_outbound → Snappy → KcpConn.write
//   KcpConn.read → Snappy decode → SMUX process_data
//
// Server multi-peer demux (DashMap by peer + KcpListener accept) is NOT
// covered here — see `accept_kcp_peer` docs.

/// Client-shaped dial: existing socket + remote + key/crypt + CLI KCP params.
///
/// Returns a ready [`KcpConn`] (AsyncRead/Write). Callers that still run the
/// legacy binary flush loop should **not** use this on the production path yet.
///
/// ```no_run
/// use kcptun_common::{derive_key, dial_kcp_session, KcpCliParams};
/// # use std::net::SocketAddr;
/// # use std::sync::Arc;
/// # fn main() {
/// # let _fut = async {
/// # let socket: Arc<kio::DatagramSocket> = Arc::new(kio::DatagramSocket::Udp(
/// #     kio::UdpSocket::connect("0.0.0.0:0".parse().unwrap(), "127.0.0.1:29900".parse().unwrap()).unwrap(),
/// # ));
/// # let remote: SocketAddr = "127.0.0.1:29900".parse().unwrap();
/// let key = derive_key("secret");
/// let params = KcpCliParams { mode: "fast3".into(), ..Default::default() };
/// let conn = dial_kcp_session(socket, remote, &key, "aes", &params).await?;
/// # Ok::<_, std::io::Error>(conn)
/// # };
/// # }
/// ```
pub async fn dial_kcp_session(
    socket: Arc<kio::DatagramSocket>,
    remote: SocketAddr,
    key: &[u8],
    crypt: &str,
    params: &crate::KcpCliParams,
) -> io::Result<KcpConn> {
    let config = params.to_kcp_config();
    // Client sockets are typically `connect()`ed to the remote.
    kcp_session_with_socket(socket, remote, key, crypt, config, true).await
}

/// Server single-peer helper: build a [`KcpConn`] for one known peer.
///
/// Intended for tests and for a future per-peer spawn after the listen socket
/// demuxes the first datagram. **Does not** implement multi-peer accept —
/// `KcpListener` remains a stub and the production server still uses its
/// DashMap-by-peer loop.
///
/// `connected`:
/// - `true`  — socket already `connect()`ed to `peer` (per-peer UDP socket).
/// - `false` — shared unconnected listen socket; KcpConn uses send_to(peer).
pub async fn accept_kcp_peer(
    socket: Arc<kio::DatagramSocket>,
    peer: SocketAddr,
    key: &[u8],
    crypt: &str,
    params: &crate::KcpCliParams,
    connected: bool,
) -> io::Result<KcpConn> {
    let config = params.to_kcp_config();
    kcp_session_with_socket(socket, peer, key, crypt, config, connected).await
}

fn resolve_one(addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve address"))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn null_encrypt_passthrough() {
        // Build a dummy socket we won't actually send on — only exercise encrypt helpers.
        let sock = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let inner = Arc::new(kio::DatagramSocket::Udp(sock));
        let ct = CryptoTransport::new(inner, b"unused-key-pad-to-32-bytes!!!!!", "null");
        assert!(!ct.has_encryption());
        let plain = vec![
            Bytes::from_static(b"hello-null"),
            Bytes::from_static(b"world"),
        ];
        let out = ct.encrypt_data(plain.clone()).await;
        assert_eq!(out.len(), 2);
        assert_eq!(&out[0][..], b"hello-null");
        assert_eq!(&out[1][..], b"world");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn aes_cfb_roundtrip_via_crypto_bufs() {
        let sock = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let inner = Arc::new(kio::DatagramSocket::Udp(sock));
        let key = b"0123456789abcdef0123456789abcdef";
        let ct = CryptoTransport::new(inner, key, "aes");
        assert!(ct.has_encryption());
        assert!(!ct.crypt.is_aead());

        let plain = b"kcp-segment-payload-for-cfb-test!!";
        let encrypted = ct.encrypt_data(vec![Bytes::copy_from_slice(plain)]).await;
        assert_eq!(encrypted.len(), 1);
        assert!(encrypted[0].len() > plain.len()); // 20B header

        let mut buf = encrypted[0].to_vec();
        let nlen = buf.len();
        let n = ct.decrypt_in_place(&mut buf, nlen).unwrap();
        assert_eq!(n, plain.len());
        assert_eq!(&buf[..n], plain);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn ack_and_data_bufs_are_independent() {
        let sock = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let inner = Arc::new(kio::DatagramSocket::Udp(sock));
        let key = b"0123456789abcdef0123456789abcdef";
        let ct = CryptoTransport::new(inner, key, "aes");

        let data = ct.encrypt_data(vec![Bytes::from_static(b"data-pkt")]).await;
        let ack = ct
            .encrypt_urgent(vec![Bytes::from_static(b"ack-pkt!")])
            .await;
        // Different session_id seeds → different nonces in first 8 bytes after
        // counter (bytes 8..16 hold session_id). Counters both start at 0 so
        // bytes 0..8 may match; session half must differ.
        assert_ne!(&data[0][8..16], &ack[0][8..16]);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn aead_roundtrip() {
        let sock = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let inner = Arc::new(kio::DatagramSocket::Udp(sock));
        let key = b"0123456789abcdef0123456789abcdef";
        let ct = CryptoTransport::new(inner, key, "aes-128-gcm");
        assert!(ct.has_encryption());
        assert!(ct.crypt.is_aead());

        let plain = b"aead-payload-test";
        let encrypted = ct.encrypt_data(vec![Bytes::copy_from_slice(plain)]).await;
        let mut buf = encrypted[0].to_vec();
        // Need room: open copies plaintext back into buf
        let nlen = buf.len();
        let n = ct.decrypt_in_place(&mut buf, nlen).unwrap();
        assert_eq!(n, plain.len());
        assert_eq!(&buf[..n], plain);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kcp_session_null_localhost_roundtrip() {
        use kio::AsyncWriteExt;

        let key = b"0123456789abcdef0123456789abcdef";
        // Bind two ports, connect both sides via kcp_session_with_socket.
        let a_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let sock_b = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));

        let cfg = KcpConfig {
            conv: 0xC0FFEE,
            mode: kcp_rs::KcpMode::Fast3,
            ..KcpConfig::default()
        };

        let mut conn_a = kcp_session_with_socket(sock_a, addr_b, key, "null", cfg.clone(), true)
            .await
            .unwrap();
        let mut conn_b = kcp_session_with_socket(sock_b, addr_a, key, "null", cfg, true)
            .await
            .unwrap();

        let payload = b"session-null-roundtrip-payload!!";
        conn_a.write_all(payload).await.unwrap();
        conn_a.flush().await.unwrap();

        let mut got = vec![0u8; payload.len()];
        read_exact(&mut conn_b, &mut got, Duration::from_secs(5)).await;
        assert_eq!(&got[..], payload);

        conn_a.close();
        conn_b.close();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kcp_session_aes_localhost_roundtrip() {
        use kio::AsyncWriteExt;

        let key = b"0123456789abcdef0123456789abcdef";
        let a_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let sock_b = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));

        let cfg = KcpConfig {
            conv: 0xAE5AE5,
            datashard: 10,
            parityshard: 3,
            ..KcpConfig::default()
        };

        let mut conn_a = kcp_session_with_socket(sock_a, addr_b, key, "aes", cfg.clone(), true)
            .await
            .unwrap();
        let mut conn_b = kcp_session_with_socket(sock_b, addr_a, key, "aes", cfg, true)
            .await
            .unwrap();

        let mut payload = vec![0u8; 8 * 1024];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        conn_a.write_all(&payload).await.unwrap();
        conn_a.flush().await.unwrap();

        let mut got = vec![0u8; payload.len()];
        read_exact(&mut conn_b, &mut got, Duration::from_secs(5)).await;
        assert_eq!(got, payload);

        conn_a.close();
        conn_b.close();
    }

    /// Client-shaped dial helper roundtrip (null crypt, CLI params).
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dial_kcp_session_client_shaped_roundtrip() {
        use kio::AsyncWriteExt;

        let key = b"0123456789abcdef0123456789abcdef";
        let a_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let sock_b = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));

        // Client-like defaults: fast3, FEC 10/3, acknodelay, rcvwnd 512.
        let params = crate::KcpCliParams {
            mode: "fast3".into(),
            mtu: 1350,
            sndwnd: 128,
            rcvwnd: 512,
            datashard: 10,
            parityshard: 3,
            acknodelay: true,
            conv: 0xD1A1_C11E,
            ..crate::KcpCliParams::default()
        };

        let mut client = dial_kcp_session(sock_a, addr_b, key, "null", &params)
            .await
            .unwrap();
        // Server single-peer helper (connected=true mirrors dial).
        let mut server = accept_kcp_peer(sock_b, addr_a, key, "null", &params, true)
            .await
            .unwrap();

        let payload = b"client-shaped-dial-helper-payload!";
        client.write_all(payload).await.unwrap();
        client.flush().await.unwrap();

        let mut got = vec![0u8; payload.len()];
        read_exact(&mut server, &mut got, Duration::from_secs(5)).await;
        assert_eq!(&got[..], payload);

        // reverse
        let reply = b"server-peer-reply-ok";
        server.write_all(reply).await.unwrap();
        server.flush().await.unwrap();
        let mut got2 = vec![0u8; reply.len()];
        read_exact(&mut client, &mut got2, Duration::from_secs(5)).await;
        assert_eq!(&got2[..], reply);

        client.close();
        server.close();
    }

    /// AES + dial/accept helpers with CLI params (manual mode knobs stored).
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dial_accept_aes_with_cli_params() {
        use kio::AsyncWriteExt;

        let key = b"0123456789abcdef0123456789abcdef";
        let a_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b_tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr_a = a_tmp.local_addr().unwrap();
        let addr_b = b_tmp.local_addr().unwrap();
        drop(a_tmp);
        drop(b_tmp);

        let sock_a = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_a, addr_b).unwrap(),
        ));
        let sock_b = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(addr_b, addr_a).unwrap(),
        ));

        let params = crate::KcpCliParams {
            mode: "fast".into(),
            mtu: 1350,
            sndwnd: 64,
            rcvwnd: 64,
            datashard: 0,
            parityshard: 0,
            acknodelay: false,
            conv: 0xAE5_C11,
            ..crate::KcpCliParams::default()
        };

        // Sanity: config helper produces expected mode.
        let cfg = params.to_kcp_config();
        assert_eq!(cfg.mode, kcp_rs::KcpMode::Fast);
        assert_eq!(cfg.datashard, 0);

        let mut a = dial_kcp_session(sock_a, addr_b, key, "aes", &params)
            .await
            .unwrap();
        let mut b = accept_kcp_peer(sock_b, addr_a, key, "aes", &params, true)
            .await
            .unwrap();

        let mut payload = vec![0u8; 4096];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        a.write_all(&payload).await.unwrap();
        a.flush().await.unwrap();
        let mut got = vec![0u8; payload.len()];
        read_exact(&mut b, &mut got, Duration::from_secs(5)).await;
        assert_eq!(got, payload);

        a.close();
        b.close();
    }

    /// M0.1 — the M1-A SMUX→KcpConn write path must sense backpressure:
    /// when the peer never ACKs, `wait_send` stays ≤ `snd_wnd` and repeated
    /// writes stall (Pending) instead of buffering unboundedly in `snd_queue`.
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kcp_conn_write_backpressure_bounds_inflight() {
        use kio::AsyncWriteExt;

        // Bind-then-drop a port so nothing listens on it: the peer never ACKs.
        let tmp = kio::UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let dead = tmp.local_addr().unwrap();
        drop(tmp);

        let sock = Arc::new(kio::DatagramSocket::Udp(
            kio::UdpSocket::connect(SocketAddr::from(([127, 0, 0, 1], 0)), dead).unwrap(),
        ));
        let key = b"0123456789abcdef0123456789abcdef";
        let cfg = KcpConfig {
            conv: 0xBA0C_B00D,
            mode: kcp_rs::KcpMode::Fast3,
            sndwnd: 4,
            rcvwnd: 64,
            ..KcpConfig::default()
        };
        let mut conn = kcp_session_with_socket(sock, dead, key, "null", cfg.clone(), true)
            .await
            .unwrap();

        let chunk = vec![0xEEu8; 4096];
        let mut accepted = 0usize;
        let mut stalled = false;
        for _ in 0..100 {
            match kio::timeout(Duration::from_millis(200), conn.write_all(&chunk)).await {
                Ok(Ok(())) => accepted += chunk.len(),
                Ok(Err(e)) => panic!("write error: {}", e),
                Err(_) => {
                    stalled = true;
                    break;
                }
            }
            // `wait_send` = snd_buf + snd_queue (Go semantics). Backpressure
            // gates on it, so it must stay bounded — a small multiple of the
            // window, never the full 100 chunks.
            assert!(
                conn.wait_send() <= (cfg.sndwnd.saturating_mul(4)) as usize,
                "wait_send {} grew unbounded (snd_wnd {})",
                conn.wait_send(),
                cfg.sndwnd
            );
        }

        assert!(
            stalled,
            "writer never stalled; accepted {accepted} bytes to a dead peer"
        );
        // The writer must stall before buffering everything: some chunks were
        // blocked by backpressure. (`wait_send` staying ≤ snd_wnd*4 above is
        // the hard bound on in-flight; write_buf may overshoot by one flush
        // cycle because the gate reads a flush-loop-cached snapshot — see
        // plan M0.1 note. M1-A bounds per-call writes, so this is bounded.)
        assert!(
            accepted < 100 * chunk.len(),
            "accepted all {accepted} bytes without stalling — no backpressure"
        );

        conn.close();
    }

    #[cfg(feature = "tokio")]
    async fn read_exact(conn: &mut KcpConn, buf: &mut [u8], limit: Duration) {
        use kio::AsyncReadExt;
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
