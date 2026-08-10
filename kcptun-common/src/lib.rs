//! Shared session helpers for kcptun binaries.
//!
//! Runtime-agnostic: key derivation, KCP mode profiles, Snappy framing,
//! rate limiter.
//! Runtime-gated (`tokio` / `smol`): pipe, snmp logger, encrypted KCP
//! transport assembly, KCP config, and optional QPP port.
//!
//! `KcptunSession` is the complete per-peer KCP + Snappy + SMUX abstraction.
//! Shared-UDP servers assemble `kcp_rs::KcpListener` + `CryptoTransport`
//! (see `kcptun-server/src/app.rs`); raw-TCP sockets are per-peer and connect
//! directly via `KcptunSession::serve_transport`.

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
mod kcp_transport;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod kcptun_session;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod snappy_pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod snmp_log;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub use kcp_config::{
    kcp_config_from, kcp_config_from_cli, parse_kcp_mode, KcpCliParams, DEFAULT_CONV,
};
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use kcp_transport::CryptoTransport;
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use kcptun_session::{KcptunConfig, KcptunSession};
#[cfg(any(feature = "tokio", feature = "smol"))]
pub use pipe::pipe;
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
