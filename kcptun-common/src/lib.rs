//! Shared session helpers for kcptun binaries.
//!
//! Runtime-agnostic: key derivation, KCP mode profiles, Snappy framing,
//! rate limiter.
//! Runtime-gated (`tokio` / `smol`): pipe, snmp logger, CryptoTransport /
//! kcp_session / dial helpers, KcpConfig from CLI, optional QPP port.
//!
//! **Note (Task 4):** production client/server still use their legacy
//! KCP+SMUX+Snappy flush loops. Prefer `dial_kcp_session` /
//! `accept_kcp_peer` + `kcp_config_from` for new code and tests; do not
//! put Snappy inside `KcpConn`.

mod key;
mod mode;
mod multiport;
mod ratelimit;
mod snappy_frame;

pub use key::derive_key;
pub use mode::apply_mode;
pub use multiport::parse_multi_port;
pub use ratelimit::RateLimiter;
pub use snappy_frame::SnappyStreamDecoder;

#[cfg(any(feature = "tokio", feature = "smol"))]
mod kcp_config;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod session;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod snappy_pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod snmp_log;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub use kcp_config::{
    kcp_config_from, kcp_config_from_cli, parse_kcp_mode, KcpCliParams, DEFAULT_CONV,
};
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use pipe::pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use session::{
    accept_kcp_peer, dial_kcp_session, kcp_session, kcp_session_with_socket, CryptoTransport,
};
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use snappy_pipe::SnappyPipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use snmp_log::snmp_logger;

#[cfg(feature = "qpp")]
mod qpp_port;
#[cfg(feature = "qpp")]
mod qpp_validate;
#[cfg(feature = "qpp")]
pub use qpp_port::QPPPort;
#[cfg(feature = "qpp")]
pub use qpp_validate::validate_qpp_params;
