use anyhow::{bail, Result};
use sha2::{Digest, Sha256};

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn verify_sha256(data: &[u8], hash_hex: &str, size: i64) -> Result<()> {
    if data.len() as i64 != size {
        bail!(
            "blob size {} does not match digest size_bytes {size}",
            data.len()
        );
    }
    let got = sha256_hex(data);
    if !hash_eq(&got, hash_hex) {
        bail!("sha256 mismatch: expected {hash_hex}, got {got}");
    }
    Ok(())
}

/// Constant-time-ish hex compare (case insensitive).
pub fn hash_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

pub fn digest_hash_hex(d: &crate::reapi::Digest) -> Result<String> {
    if d.hash.is_empty() {
        bail!("empty digest hash");
    }
    Ok(d.hash.clone())
}
