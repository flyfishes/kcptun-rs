//! Tests for qpp-rs — encryption roundtrip, determinism, PRNG.
//!
//! These tests match the original tests from [crate] documentation.

use crate::{create_prng, QuantumPermutationPad};

#[test]
fn qpp_roundtrip() {
    let key = b"test-key-12345-test-key-67890";
    let mut qpp = QuantumPermutationPad::new(key, 61);
    let mut data = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90];
    let original = data.clone();
    qpp.encrypt(&mut data);
    assert_ne!(data, original);
    qpp.decrypt(&mut data);
    assert_eq!(data, original);
}

#[test]
fn qpp_empty() {
    let mut qpp = QuantumPermutationPad::new(b"key", 10);
    let mut empty: Vec<u8> = vec![];
    qpp.encrypt(&mut empty);
    assert!(empty.is_empty());
    qpp.decrypt(&mut empty);
    assert!(empty.is_empty());
}

#[test]
fn qpp_pad_count() {
    assert_eq!(QuantumPermutationPad::new(b"key", 61).count(), 61);
}

#[test]
fn qpp_deterministic() {
    let key = b"deterministic-test-key-for-qpp";
    let data = b"hello qpp world!";
    let mut qpp1 = QuantumPermutationPad::new(key, 10);
    let mut qpp2 = QuantumPermutationPad::new(key, 10);
    let mut d1 = data.to_vec();
    let mut d2 = data.to_vec();
    qpp1.encrypt(&mut d1);
    qpp2.encrypt(&mut d2);
    assert_eq!(d1, d2);
}

#[test]
fn prng_deterministic() {
    let seed = b"test-seed-for-prng";
    let mut rng1 = create_prng(seed);
    let mut rng2 = create_prng(seed);
    for _ in 0..100 {
        assert_eq!(rng1.next_u64(), rng2.next_u64());
    }
}

#[test]
fn xoshiro_works() {
    use crate::prng::xoshiro256ss;
    let mut state = [1u64, 2, 3, 4];
    assert!(xoshiro256ss(&mut state) != 0);
}

#[test]
fn long_data_roundtrip() {
    let key = b"test-long-data-key";
    let mut qpp = QuantumPermutationPad::new(key, 31);
    let mut data: Vec<u8> = (0..1000).map(|i| (i & 0xFF) as u8).collect();
    let original = data.clone();
    qpp.encrypt(&mut data);
    assert_ne!(data, original);
    qpp.decrypt(&mut data);
    assert_eq!(data, original);
}
