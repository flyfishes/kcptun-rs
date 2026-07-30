//! Shared session helpers for kcptun binaries.
//!
//! Runtime-agnostic: key derivation, KCP mode profiles, Snappy framing,
//! rate limiter.
//! Runtime-gated (`tokio` / `smol`): pipe, snmp logger, optional QPP port.

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
mod pipe;
#[cfg(any(feature = "tokio", feature = "smol"))]
mod snmp_log;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub use pipe::pipe;
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
