//! End-to-end data-integrity tests for the async [`KcpConn`] (`feature = "async"`).
//!
//! Run on their own with either runtime backend:
//!
//! ```text
//! cargo test -p kcp-rs --features async-tokio --test kcpconn_integrity
//! cargo test -p kcp-rs --features async-smol  --test kcpconn_integrity
//! ```
//!
//! Two real [`KcpConn`]s over localhost UDP transfer large deterministic
//! payloads (with and without Reed-Solomon FEC); the received bytes must be
//! byte-for-byte identical to what was written.

#![cfg(any(feature = "async-tokio", feature = "async-smol"))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kcp_rs::{KcpConn, KcpMode, PacketTransport};
use kio::{AsyncReadExt, AsyncWriteExt};

/// Deterministic, non-trivial payload pattern.
fn make_payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 131 + seed as usize) % 251) as u8)
        .collect()
}

/// FNV-1a 64 — independent checksum cross-check on top of byte equality.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// Build a connected pair of `KcpConn` over localhost UDP, optionally with FEC.
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
    .conv(0x00C0_FFEE)
    .mode(KcpMode::Fast3)
    .mtu(1350)
    .sndwnd(512)
    .rcvwnd(512);
    let mut bb = KcpConn::with_transport(
        Arc::new(kio::DatagramSocket::Udp(sock_b)) as Arc<dyn PacketTransport>,
        addr_a,
    )
    .connected(true)
    .conv(0x00C0_FFEE)
    .mode(KcpMode::Fast3)
    .mtu(1350)
    .sndwnd(512)
    .rcvwnd(512);
    if let Some((d, p)) = fec {
        ba = ba.fec(d, p);
        bb = bb.fec(d, p);
    }
    let conn_a = ba.build().await.unwrap();
    let conn_b = bb.build().await.unwrap();
    (conn_a, conn_b)
}

/// Write `payload` end-to-end and verify the peer reads it back intact.
async fn roundtrip(from: &mut KcpConn, to: &mut KcpConn, payload: &[u8]) {
    from.write_all(payload).await.unwrap();
    from.flush().await.unwrap();
    let mut got = vec![0u8; payload.len()];
    read_exact_timeout(to, &mut got, Duration::from_secs(30)).await;
    assert_eq!(got, payload, "byte-for-byte mismatch");
    assert_eq!(fnv1a(&got), fnv1a(payload), "checksum mismatch");
}

/// Read exactly `buf.len()` bytes, polling with a timeout.
async fn read_exact_timeout(conn: &mut KcpConn, buf: &mut [u8], limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    let mut filled = 0usize;
    while filled < buf.len() {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for data, got {}/{}", filled, buf.len());
        }
        match kio::timeout(Duration::from_millis(200), conn.read(&mut buf[filled..])).await {
            Ok(Ok(0)) => panic!("unexpected EOF at {}", filled),
            Ok(Ok(n)) => filled += n,
            Ok(Err(e)) => panic!("read error: {}", e),
            Err(_) => continue,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Bidirectional integrity over localhost UDP (no FEC).
#[test]
fn kcpconn_bidirectional_integrity() {
    kio::block_on(async {
        let (mut conn_a, mut conn_b) = pair_conns(None).await;
        let payload = make_payload(256 * 1024, 11);
        roundtrip(&mut conn_a, &mut conn_b, &payload).await;
        let payload2 = make_payload(512 * 1024, 77);
        roundtrip(&mut conn_b, &mut conn_a, &payload2).await;
        conn_a.close();
        conn_b.close();
    });
}

/// Integrity with Reed-Solomon FEC 10/3 (Go-compatible defaults).
#[test]
fn kcpconn_fec_integrity() {
    kio::block_on(async {
        let (mut conn_a, mut conn_b) = pair_conns(Some((10, 3))).await;
        let payload = make_payload(512 * 1024, 23);
        roundtrip(&mut conn_a, &mut conn_b, &payload).await;
        conn_a.close();
        conn_b.close();
    });
}
