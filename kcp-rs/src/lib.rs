//! # kcp-rs
//!
//! A high-performance Rust implementation of the KCP (KCP Protocol) reliable
//! UDP transport. KCP is a fast ARQ (Automatic Repeat-reQuest) protocol that
//! provides reliable, ordered, and connection-oriented data delivery over UDP.
//!
//! ## Design
//!
//! - **Zero-copy** segment parsing via `bytes::BytesMut`
//! - **Lock-free** segment pooling with `crossbeam::queue::SegQueue`
//! - **Atomic SNMP counters** via `std::sync::atomic` with precise `Ordering`
//! - **Cache-friendly** `#[repr(C)]` struct layouts aligned to 64-byte cache lines
//! - **Pluggable** `BlockCrypt` trait for encryption at the segment level
//! - **Reed-Solomon FEC** for forward error correction
//!
//! ## Encryption
//!
//! Block-cipher / AEAD engines and **wire packing** (`CryptoBuf`,
//! `encrypt_batch`, offload heuristics) live in [`kcrypt-rs`](../kcrypt_rs).
//! This crate does **not** depend on it — depend on `kcrypt-rs` directly for
//! anything crypto related.

// The KCP state machine is a close port of Go's kcp-go v5 and intentionally
// mirrors the upstream control flow for easy auditing. Several clippy lints
// (collapsible-if, while-let, type-complexity, etc.) would obscure that
// correspondence, so they are suppressed at the crate level here.
#![allow(
    clippy::type_complexity,
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::while_let_loop,
    clippy::manual_div_ceil,
    clippy::unnecessary_cast,
    clippy::needless_range_loop,
    clippy::redundant_pattern_matching,
    clippy::uninlined_format_args,
    clippy::too_many_arguments,
    clippy::new_without_default,
    clippy::len_without_is_empty,
    clippy::empty_line_after_doc_comments,
    clippy::absurd_extreme_comparisons,
    clippy::if_same_then_else,
    clippy::manual_range_contains,
    clippy::useless_conversion,
    clippy::arc_with_non_send_sync,
    clippy::needless_late_init,
    clippy::manual_hash_one,
    clippy::collapsible_else_if,
    // Newer clippy lints on pre-existing KCP/crypto paths (Go-port fidelity).
    clippy::manual_clamp,
    clippy::manual_checked_ops,
)]

pub mod config;
pub mod fec;
pub mod kcp;
pub mod segment;
pub mod snmp;

#[cfg(any(feature = "async-tokio", feature = "async-smol"))]
pub mod conn;

pub use config::{KcpConfig, KcpMode, DEFAULT_CONV};
pub use fec::{
    fec_expand_packets, fec_kcp_from_recovered, FecDecoder, FecEncoder, FEC_HEADER_SIZE,
    FEC_TYPE_DATA, FEC_TYPE_PARITY,
};
pub use kcp::KCP;
pub use segment::SegmentPool;
pub use snmp::{add as snmp_add, enable as snmp_enable, store as snmp_store, DEFAULT_SNMP, SNMP};

#[cfg(any(feature = "async-tokio", feature = "async-smol"))]
pub use conn::{
    KcpConn, KcpConnBuilder, KcpListener, KcpListenerBuilder, KcpTcpListener,
    KcpTcpListenerBuilder, PacketTransport,
};
