//! Server socket creation: UDP socket with buffer size and DSCP.

use std::net::SocketAddr;

use anyhow::{Context as AnyContext, Result};
use log::warn;

/// Parse a "host:port" string into a SocketAddr.
#[allow(dead_code)]
pub(crate) fn parse_addr(addr: &str) -> Result<SocketAddr> {
    // Handle ":port" shorthand by defaulting to "0.0.0.0"
    if addr.starts_with(':') {
        let host_addr = format!("0.0.0.0{}", addr);
        return host_addr.parse::<SocketAddr>().context("invalid address");
    }
    addr.parse::<SocketAddr>().context("invalid address")
}

/// Create a UDP socket bound to `addr` with the given buffer sizes and DSCP.
pub(crate) fn create_udp_socket(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
) -> Result<kio::UdpSocket> {
    build_udp(addr, sockbuf, dscp, false)
}

/// Create a **SO_REUSEPORT** UDP socket bound to `addr` (Linux: kernel hashes
/// each peer to one socket among the shards; per-socket worker threads then
/// own their fd with no shared-socket send contention).
pub(crate) fn create_udp_socket_shard(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
) -> Result<kio::UdpSocket> {
    build_udp(addr, sockbuf, dscp, true)
}

fn build_udp(
    addr: SocketAddr,
    sockbuf: u32,
    dscp: u32,
    reuse_port: bool,
) -> Result<kio::UdpSocket> {
    let socket = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::DGRAM,
        None,
    )?;
    if let Err(e) = socket.set_recv_buffer_size(sockbuf as usize) {
        warn!("set_recv_buffer_size failed: {}", e);
    }
    if let Err(e) = socket.set_send_buffer_size(sockbuf as usize) {
        warn!("set_send_buffer_size failed: {}", e);
    }
    if reuse_port {
        // SO_REUSEPORT: allow N sockets to bind the same addr:port; the kernel
        // distributes inbound datagrams across them (Linux: by connection hash).
        if let Err(e) = socket.set_reuse_port(true) {
            warn!("set_reuse_port failed: {}", e);
        }
    }
    if dscp > 0 {
        let dscp_shifted = dscp << 2;
        if let Err(e) = socket.set_tos(dscp_shifted) {
            warn!("set_tos (DSCP) failed: {}", e);
        }
    }
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(kio::UdpSocket::from_std(socket.into())?)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_addr() {
        let addr = parse_addr("127.0.0.1:29900").unwrap();
        assert_eq!(addr.port(), 29900);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn test_parse_addr_ipv6() {
        let addr = parse_addr("[::1]:29900").unwrap();
        assert_eq!(addr.port(), 29900);
    }

    #[test]
    fn test_parse_addr_invalid() {
        assert!(parse_addr("not-an-address").is_err());
    }
}
