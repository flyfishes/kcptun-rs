//! # qpp-rs
//!
//! Quantum Permutation Pad (QPP) encryption — a port of Go's `xtaci/qpp` that
//! preserves **algorithmic compatibility**.
//!
//! ## Wire-level compatibility
//!
//! This implementation produces the exact same encrypted output as Go's qpp
//! given the same key, data, and pad configuration:
//! - xoshiro256** PRNG (matching Go's xoshiro256ss)
//! - PBKDF2(SHA1, 128 rounds) for key derivation
//! - PAD_SWITCH=8 bytes per pad before switching
//! - Permutation via AES-256 encrypted Fisher-Yates shuffle

mod cipher;
mod pad;
mod prng;

pub use pad::{decrypt_with_pads, encrypt_with_pads, QuantumPermutationPad};
pub use prng::{create_prng, Rand};

#[cfg(test)]
mod tests;
