/// PBKDF2 salt matching the Go kcp-go SALT value (`b"kcp-go"`).
const SALT: &[u8] = b"kcp-go";

/// Derive a 32-byte key from a password using PBKDF2-HMAC-SHA1.
///
/// Matches the Go kcp-go key derivation:
/// `pkcs5.PBKDF2(password, salt, 4096, 32, sha1.New)`
pub fn derive_key(password: &str) -> [u8; 32] {
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha1::Sha1>(password.as_bytes(), SALT, 4096, &mut key);
    key
}
