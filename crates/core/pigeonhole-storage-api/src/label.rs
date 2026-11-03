//! What every store takes as a blob's name and as a chain member's label,
//! and what it takes as a blob: one rule, checked by [`check_blob`].
//!
//! A name ([`crate::BlobBackend::put`]) or a label ([`crate::Chain`]) is 1
//! to [`MAX_LABEL`] ASCII letters, digits, `-`, `_` and `.`. A store keeps
//! it readable without the data (a file name, a text message's header, a
//! record header) and builds on that alphabet: Telegram and Discord name a
//! member `<label>~<prev>.bin`, a text message starts `<name> <len>\n`, so
//! `~`, spaces and newlines are never in one. The data of a blob is never
//! empty (a chat refuses an empty document) and fits the store's
//! `max_blob_size`.

use anyhow::{bail, Result};
use pigeonhole_types::BackendLimits;

/// The longest name or label.
pub const MAX_LABEL: usize = 128;

/// Whether `label` is a name or label every store takes.
pub fn check_label(label: &str) -> Result<()> {
    let ok = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    if label.is_empty() || label.len() > MAX_LABEL || !label.chars().all(ok) {
        bail!("a blob name or label is 1 to {MAX_LABEL} of [A-Za-z0-9._-]: {label:?}");
    }
    Ok(())
}

/// Whether a store takes `data` as a blob named (or labelled) `name`: what
/// every put checks first.
pub fn check_blob(name: &str, data: &[u8], limits: &BackendLimits) -> Result<()> {
    check_label(name)?;
    if data.is_empty() {
        bail!("{name}: a blob is never empty");
    }
    if data.len() > limits.max_blob_size {
        bail!("{name}: blob size {} exceeds the store's max {}", data.len(), limits.max_blob_size);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_types::RangeSupport;

    #[test]
    fn names_and_labels_share_one_alphabet() {
        for ok in ["ph-0123abcd.bin", "log-claim-7", "snapshot-1-20.snap", "owner-deadbeef.bin", "a_b"] {
            check_label(ok).unwrap();
        }
        let long = "x".repeat(MAX_LABEL + 1);
        for bad in ["", "a~b", "a b", "a\nb", "ä", "a/b", long.as_str()] {
            assert!(check_label(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_blob_is_never_empty_and_fits() {
        let limits = BackendLimits { max_blob_size: 4, supports_range: RangeSupport::BestEffort };
        check_blob("a.bin", b"abcd", &limits).unwrap();
        assert!(check_blob("a.bin", b"", &limits).is_err());
        assert!(check_blob("a.bin", b"abcde", &limits).is_err());
        assert!(check_blob("a bin", b"a", &limits).is_err());
    }
}
