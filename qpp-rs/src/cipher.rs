//! Seed expansion and Fisher-Yates shuffle — matching Go's qpp.

use crate::prng::{self, qpp_minimum_seed_length_inner, CHUNK_DERIVE_LOOPS, CHUNK_DERIVE_SALT};

/// Split and expand a seed into chunks using PBKDF2 (matching Go).
pub(crate) fn seed_to_chunks(seed: &[u8]) -> Vec<Vec<u8>> {
    let seed = if seed.len() < 32 {
        let mut expanded = vec![0u8; 32];
        let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha1::Sha1>>(
            seed,
            CHUNK_DERIVE_SALT.as_bytes(),
            CHUNK_DERIVE_LOOPS,
            &mut expanded,
        );
        expanded
    } else {
        seed.to_vec()
    };

    // QPPMinimumSeedLength(QUBITS=8): 256! needs ~211 bytes
    let byte_length = qpp_minimum_seed_length_inner(prng::QUBITS);
    let chunk_count = (byte_length.div_ceil(32)).max(1);
    let mut chunks = vec![vec![0u8; 32]; chunk_count];
    for i in 0..chunk_count {
        for j in 0..32 {
            chunks[i][j] = seed[(i * 32 + j) % seed.len()];
        }
        let mut derived = vec![0u8; 32];
        let _ = pbkdf2::pbkdf2::<hmac::Hmac<sha1::Sha1>>(
            &chunks[i],
            CHUNK_DERIVE_SALT.as_bytes(),
            CHUNK_DERIVE_LOOPS,
            &mut derived,
        );
        chunks[i] = derived;
    }
    chunks
}

/// Fisher-Yates shuffle using AES-256 encrypted randomness (matching Go).
pub(crate) fn shuffle_pad(chunk: &[u8], pad: &mut [u8], pad_id: u16, blocks: &[aes::Aes256]) {
    use aes::cipher::generic_array::GenericArray;
    use aes::cipher::BlockEncrypt;
    use hmac::Mac;

    let message = format!("QPP_{:b}", pad_id);
    let mut mac = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(chunk).unwrap();
    mac.update(message.as_bytes());
    let mut sum = mac.finalize().into_bytes();

    for i in (1..pad.len()).rev() {
        // Go: encrypt sum with ALL AES blocks, ALL 32 bytes, in shuffle loop
        for b in blocks {
            for off in (0..sum.len()).step_by(16) {
                let mut block_data = GenericArray::clone_from_slice(&sum[off..off + 16]);
                b.encrypt_block(&mut block_data);
                sum[off..off + 16].copy_from_slice(&block_data);
            }
        }

        let bigrand = {
            let mut val = 0u64;
            for &b in sum.iter().take(8) {
                val = (val << 8) | b as u64;
            }
            val % (i + 1) as u64
        };

        pad.swap(i, bigrand as usize);
    }
}
