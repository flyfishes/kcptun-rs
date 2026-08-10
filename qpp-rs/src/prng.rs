//! xoshiro256** PRNG and pad utilities — matching Go's qpp.

use std::fmt;

// ─── Constants (matching Go qpp) ──────────────────────────────────────────────

pub(crate) const PM_SELECTOR_IDENTIFIER: &str = "PERMUTATION_MATRIX_SELECTOR";
pub(crate) const SHUFFLE_SALT: &str = "___QUANTUM_PERMUTATION_PAD_SHUFFLE_SALT___";
pub(crate) const PRNG_SALT: &str = "___QUANTUM_PERMUTATION_PAD_PRNG_SALT___";
pub(crate) const CHUNK_DERIVE_SALT: &str = "___QUANTUM_PERMUTATION_PAD_SEED_DERIVE___";
pub(crate) const PBKDF2_LOOPS: u32 = 128;
pub(crate) const CHUNK_DERIVE_LOOPS: u32 = 1024;
pub(crate) const PAD_SWITCH: u8 = 8;
pub(crate) const QUBITS: u8 = 8;

// ─── Rand (xoshiro256** PRNG) ─────────────────────────────────────────────────

/// Stateful xoshiro256** PRNG matching Go's qpp.Rand.
#[derive(Clone)]
pub struct Rand {
    pub(crate) xoshiro: [u64; 4],
    pub(crate) seed64: u64,
    pub(crate) count: u8,
}

impl fmt::Debug for Rand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rand")
            .field("xoshiro", &self.xoshiro)
            .field("seed64", &self.seed64)
            .field("count", &self.count)
            .finish()
    }
}

#[inline]
fn rol64(x: u64, k: u32) -> u64 {
    x.rotate_left(k)
}

/// xoshiro256** step — matches Go's xoshiro256ss.
#[inline]
pub(crate) fn xoshiro256ss(s: &mut [u64; 4]) -> u64 {
    let result = rol64(s[1].wrapping_mul(5), 7).wrapping_mul(9);
    let t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rol64(s[3], 45);
    result
}

impl Rand {
    /// Step the PRNG and return the next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        let r = xoshiro256ss(&mut self.xoshiro);
        self.seed64 = r;
        r
    }
}

/// Create a PRNG from a seed using PBKDF2 (matching Go's CreatePRNG).
pub fn create_prng(seed: &[u8]) -> Rand {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(seed).unwrap();
    mac.update(PM_SELECTOR_IDENTIFIER.as_bytes());
    let sum = mac.finalize().into_bytes();

    // PBKDF2(SHA1, 128 rounds) to derive xoshiro state
    let mut xoshiro_key = [0u8; 32];
    let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha1::Sha1>>(
        &sum,
        PRNG_SALT.as_bytes(),
        PBKDF2_LOOPS,
        &mut xoshiro_key,
    );

    let mut xoshiro = [0u64; 4];
    xoshiro[0] = u64::from_le_bytes(xoshiro_key[0..8].try_into().unwrap());
    xoshiro[1] = u64::from_le_bytes(xoshiro_key[8..16].try_into().unwrap());
    xoshiro[2] = u64::from_le_bytes(xoshiro_key[16..24].try_into().unwrap());
    xoshiro[3] = u64::from_le_bytes(xoshiro_key[24..32].try_into().unwrap());

    let seed64 = xoshiro256ss(&mut xoshiro);
    Rand {
        xoshiro,
        seed64,
        count: 0,
    }
}

/// Compute minimum seed byte length for a given permutation size (Ω(n!) bits).
pub(crate) fn qpp_minimum_seed_length_inner(qubits: u8) -> usize {
    let n = 1usize << qubits; // e.g., 256 for QUBITS=8
    let mut bits = 0.0f64;
    for i in 2..=n {
        bits += (i as f64).log2();
    }
    (bits.ceil() as usize).div_ceil(8)
}
