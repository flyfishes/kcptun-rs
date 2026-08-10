//! Quantum Permutation Pad encryption/decryption — matching Go's qpp.

use std::fmt;

use crate::cipher::{seed_to_chunks, shuffle_pad};
use crate::prng::{self, create_prng, xoshiro256ss, Rand, PAD_SWITCH, PBKDF2_LOOPS, QUBITS};

/// A quantum permutation pad (QPP) encryption/decryption device.
///
/// Wire-compatible with Go's `xtaci/qpp`.
pub struct QuantumPermutationPad {
    /// Encryption pads (forward permutations).
    pub pads: Vec<u8>,
    /// Decryption pads (reverse permutations).
    pub rpads: Vec<u8>,
    num_pads: u16,
    enc_rand: Rand,
    dec_rand: Rand,
}

impl fmt::Debug for QuantumPermutationPad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuantumPermutationPad")
            .field("num_pads", &self.num_pads)
            .finish()
    }
}

impl QuantumPermutationPad {
    /// Create a new QPP with the given key and pad count.
    pub fn new(key: &[u8], num_pads: u16) -> Self {
        use aes::cipher::KeyInit;
        let num_pads = num_pads.max(1);
        let matrix_bytes = 1 << QUBITS;
        let total = num_pads as usize * matrix_bytes;
        let mut pads = vec![0u8; total];
        let mut rpads = vec![0u8; total];

        let chunks = seed_to_chunks(key);
        // Create AES-256 blocks for each chunk (matching Go)
        let mut blocks: Vec<aes::Aes256> = Vec::new();
        for chunk in &chunks {
            let mut aes_key = [0u8; 32];
            let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha1::Sha1>>(
                chunk,
                prng::SHUFFLE_SALT.as_bytes(),
                PBKDF2_LOOPS,
                &mut aes_key,
            );
            blocks.push(aes::Aes256::new_from_slice(&aes_key).unwrap());
        }

        for i in 0..num_pads as usize {
            let pad = &mut pads[i * matrix_bytes..(i + 1) * matrix_bytes];
            for (j, slot) in pad.iter_mut().enumerate() {
                *slot = j as u8;
            }
            shuffle_pad(&chunks[i % chunks.len()], pad, i as u16, &blocks);
            let rpad = &mut rpads[i * matrix_bytes..(i + 1) * matrix_bytes];
            for (j, slot) in pad.iter().enumerate() {
                rpad[*slot as usize] = j as u8;
            }
        }

        let enc_rand = create_prng(key);
        let dec_rand = create_prng(key);

        QuantumPermutationPad {
            pads,
            rpads,
            num_pads,
            enc_rand,
            dec_rand,
        }
    }

    /// Encrypt data in-place using the default PRNG.
    pub fn encrypt(&mut self, data: &mut [u8]) {
        encrypt_with_pads(&self.pads, data, &mut self.enc_rand, self.num_pads);
    }

    /// Decrypt data in-place using the default PRNG.
    pub fn decrypt(&mut self, data: &mut [u8]) {
        decrypt_with_pads(&self.rpads, data, &mut self.dec_rand, self.num_pads);
    }

    /// Get the number of pads.
    #[inline]
    pub fn count(&self) -> u16 {
        self.num_pads
    }
}

/// Internal: encrypt data using permutation pads.
pub fn encrypt_with_pads(pads: &[u8], data: &mut [u8], rand: &mut Rand, num_pads: u16) {
    if data.is_empty() || num_pads == 0 {
        return;
    }
    let size = data.len();
    let mut r: u64 = rand.seed64;
    let mut base = (r as u16 % num_pads) as usize * 256;
    let mut count = rand.count;
    let mut offset = 0usize;

    if count != 0 {
        while offset < data.len() {
            let rr = (r >> (count * 8)) as u8;
            data[offset] = pads[base + (data[offset] ^ rr) as usize];
            count += 1;
            if count == PAD_SWITCH {
                r = xoshiro256ss(&mut rand.xoshiro);
                base = (r as u16 % num_pads) as usize * 256;
                offset += 1;
                count = 0;
                break;
            }
            offset += 1;
        }
    }

    let remaining = &mut data[offset..];
    let repeat = remaining.len() / 8;
    for i in 0..repeat {
        let d = &mut remaining[i * 8..i * 8 + 8];
        let rr0 = r as u8;
        let rr1 = (r >> 8) as u8;
        let rr2 = (r >> 16) as u8;
        let rr3 = (r >> 24) as u8;
        let rr4 = (r >> 32) as u8;
        let rr5 = (r >> 40) as u8;
        let rr6 = (r >> 48) as u8;
        let rr7 = (r >> 56) as u8;

        d[0] = pads[base + (d[0] ^ rr0) as usize];
        d[1] = pads[base + (d[1] ^ rr1) as usize];
        d[2] = pads[base + (d[2] ^ rr2) as usize];
        d[3] = pads[base + (d[3] ^ rr3) as usize];
        d[4] = pads[base + (d[4] ^ rr4) as usize];
        d[5] = pads[base + (d[5] ^ rr5) as usize];
        d[6] = pads[base + (d[6] ^ rr6) as usize];
        d[7] = pads[base + (d[7] ^ rr7) as usize];

        r = xoshiro256ss(&mut rand.xoshiro);
        base = (r as u16 % num_pads) as usize * 256;
    }

    let tail_start = offset + repeat * 8;
    for i in tail_start..data.len() {
        let rr = (r >> (count * 8)) as u8;
        data[i] = pads[base + (data[i] ^ rr) as usize];
        count += 1;
    }

    rand.seed64 = r;
    rand.count = ((rand.count as usize + size) % PAD_SWITCH as usize) as u8;
}

/// Internal: decrypt data using reverse permutation pads.
pub fn decrypt_with_pads(rpads: &[u8], data: &mut [u8], rand: &mut Rand, num_pads: u16) {
    if data.is_empty() || num_pads == 0 {
        return;
    }
    let size = data.len();
    let mut r: u64 = rand.seed64;
    let mut base = (r as u16 % num_pads) as usize * 256;
    let mut count = rand.count;
    let mut offset = 0usize;

    if count != 0 {
        while offset < data.len() {
            let rr = (r >> (count * 8)) as u8;
            data[offset] = rpads[base + data[offset] as usize] ^ rr;
            count += 1;
            if count == PAD_SWITCH {
                r = xoshiro256ss(&mut rand.xoshiro);
                base = (r as u16 % num_pads) as usize * 256;
                offset += 1;
                count = 0;
                break;
            }
            offset += 1;
        }
    }

    let remaining = &mut data[offset..];
    let repeat = remaining.len() / 8;
    for i in 0..repeat {
        let d = &mut remaining[i * 8..i * 8 + 8];
        let rr0 = r as u8;
        let rr1 = (r >> 8) as u8;
        let rr2 = (r >> 16) as u8;
        let rr3 = (r >> 24) as u8;
        let rr4 = (r >> 32) as u8;
        let rr5 = (r >> 40) as u8;
        let rr6 = (r >> 48) as u8;
        let rr7 = (r >> 56) as u8;

        d[0] = rpads[base + d[0] as usize] ^ rr0;
        d[1] = rpads[base + d[1] as usize] ^ rr1;
        d[2] = rpads[base + d[2] as usize] ^ rr2;
        d[3] = rpads[base + d[3] as usize] ^ rr3;
        d[4] = rpads[base + d[4] as usize] ^ rr4;
        d[5] = rpads[base + d[5] as usize] ^ rr5;
        d[6] = rpads[base + d[6] as usize] ^ rr6;
        d[7] = rpads[base + d[7] as usize] ^ rr7;

        r = xoshiro256ss(&mut rand.xoshiro);
        base = (r as u16 % num_pads) as usize * 256;
    }

    let tail_start = offset + repeat * 8;
    for i in tail_start..data.len() {
        let rr = (r >> (count * 8)) as u8;
        data[i] = rpads[base + data[i] as usize] ^ rr;
        count += 1;
    }

    rand.seed64 = r;
    rand.count = ((rand.count as usize + size) % PAD_SWITCH as usize) as u8;
}
