//! Multi-port address parser (Go `ParseMultiPort`).
//!
//! Parses "host:port" and "host:minport-maxport" strings into a list of
//! [`SocketAddr`]. Shared by client dial and server listen paths.

use std::net::SocketAddr;

use anyhow::{Context, Result};

/// Parse a "host:minport-maxport" or "host:port" string into a list of SocketAddr.
/// Supports ":port" shorthand where host defaults to "0.0.0.0".
pub fn parse_multi_port(addr: &str) -> Result<Vec<SocketAddr>> {
    let colon = addr.rfind(':').context("address must include host:port")?;
    let host = if colon == 0 {
        "0.0.0.0"
    } else {
        &addr[..colon]
    };
    let port_spec = &addr[colon + 1..];

    if let Some(dash) = port_spec.find('-') {
        let min_port: u16 = port_spec[..dash].parse()?;
        let max_port: u16 = port_spec[dash + 1..].parse()?;
        if min_port > max_port || min_port == 0 || max_port == 0 {
            anyhow::bail!(
                "invalid port range: minport={} -> maxport={}",
                min_port,
                max_port
            );
        }
        let mut addrs = Vec::with_capacity((max_port - min_port + 1) as usize);
        for port in min_port..=max_port {
            addrs.push(format!("{}:{}", host, port).parse()?);
        }
        Ok(addrs)
    } else {
        let port: u16 = port_spec.parse()?;
        Ok(vec![format!("{}:{}", host, port).parse()?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_port() {
        let addrs = parse_multi_port("127.0.0.1:1234").unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].port(), 1234);
    }

    #[test]
    fn test_port_range() {
        let addrs = parse_multi_port("127.0.0.1:1000-1003").unwrap();
        assert_eq!(addrs.len(), 4);
        assert_eq!(addrs[0].port(), 1000);
        assert_eq!(addrs[3].port(), 1003);
    }

    #[test]
    fn test_shorthand_host() {
        let addrs = parse_multi_port(":29900-29901").unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].port(), 29900);
        assert_eq!(addrs[1].port(), 29901);
    }

    #[test]
    fn test_ipv6() {
        let addrs = parse_multi_port("[::1]:1234").unwrap();
        assert_eq!(addrs.len(), 1);
    }

    #[test]
    fn test_invalid_range() {
        assert!(parse_multi_port("127.0.0.1:10-5").is_err());
        assert!(parse_multi_port("127.0.0.1:0-10").is_err());
        assert!(parse_multi_port("127.0.0.1:99999-99999").is_err());
    }

    #[test]
    fn test_no_port() {
        assert!(parse_multi_port("127.0.0.1").is_err());
    }
}
