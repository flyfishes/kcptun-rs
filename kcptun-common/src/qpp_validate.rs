//! QPP parameter validation (matching Go kcptun's `ValidateQPPParams`).
//!
//! Checks QPPCount and key length for safe QPP configuration.
//! Only available with the `qpp` feature.

/// Minimum seed length for QPP (2^QUBITS = 256 bytes, matching Go qpp).
const QPP_MIN_SEED_LENGTH: usize = 256;

/// Validate QPP parameters and return warnings for unsafe configurations.
///
/// Returns `Ok(warnings)` with a list of non-fatal warning messages, or
/// `Err(message)` for a fatal configuration error.
///
/// Checks performed (matching Go's `ValidateQPPParams`):
/// - QPPCount must be > 0 (fatal)
/// - Key must be at least `QPP_MIN_SEED_LENGTH` bytes (warning)
/// - QPPCount should meet minimum pad requirements (warning)
/// - QPPCount should be prime relative to 256 (warning)
pub fn validate_qpp_params(count: u16, key: &[u8]) -> Result<Vec<String>, String> {
    if count == 0 {
        return Err("QPPCount must be greater than 0 when QPP is enabled".to_string());
    }

    let mut warnings = Vec::new();

    if key.len() < QPP_MIN_SEED_LENGTH {
        warnings.push(format!(
            "QPP Warning: 'key' has size of {} bytes, required {} bytes at least",
            key.len(),
            QPP_MIN_SEED_LENGTH
        ));
    }

    // Minimum pads check: with QUBITS=8, need at least a few pads for
    // meaningful permutation diversity.
    const MIN_PADS: u16 = 8;
    if count < MIN_PADS {
        warnings.push(format!(
            "QPP Warning: QPPCount {}, required {} at least",
            count, MIN_PADS
        ));
    }

    // Prime check: GCD(count, 256) should be 1 for security.
    if gcd(count as u64, 256) != 1 {
        warnings.push(format!(
            "QPP Warning: QPPCount {}, choose a prime number for security",
            count
        ));
    }

    Ok(warnings)
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_count_is_fatal() {
        let result = validate_qpp_params(0, &[0u8; 256]);
        assert!(result.is_err());
    }

    #[test]
    fn short_key_warns() {
        let result = validate_qpp_params(61, b"short-key");
        assert!(result.is_ok());
        let warnings = result.unwrap();
        assert!(warnings.iter().any(|w| w.contains("key")));
    }

    #[test]
    fn adequate_key_no_warnings() {
        let result = validate_qpp_params(61, &[0u8; 256]);
        assert!(result.is_ok());
        let warnings = result.unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn non_prime_warns() {
        let result = validate_qpp_params(64, &[0u8; 256]); // 64 shares factor with 256
        assert!(result.is_ok());
        let warnings = result.unwrap();
        assert!(warnings.iter().any(|w| w.contains("prime")));
    }

    #[test]
    fn too_few_pads_warns() {
        let result = validate_qpp_params(3, &[0u8; 256]);
        assert!(result.is_ok());
        let warnings = result.unwrap();
        assert!(warnings.iter().any(|w| w.contains("required")));
    }
}
