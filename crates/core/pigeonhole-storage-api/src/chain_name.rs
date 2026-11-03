//! How a chat store (Telegram, Discord) names its chain members: a member
//! put after `prev` is named `<label>~<prev>.bin`, so a reader learns what
//! it follows from the name alone, readable without the data (a file name,
//! a text message's first line). The first member of a chain follows the
//! store's **anchor**, a message the store posts once and never prunes, so
//! every member names a `prev` (docs/chain-log.md). A label follows the one
//! rule of [`crate::check_label`], which keeps `~` out of it.

use std::fmt::Display;
use std::str::FromStr;

/// What every member's name ends with.
const SUFFIX: &str = ".bin";
/// Between the label and `prev`.
const SEPARATOR: char = '~';

/// The name of a member labelled `label` put after `prev`.
pub fn chain_name(label: &str, prev: impl Display) -> String {
    format!("{label}{SEPARATOR}{prev}{SUFFIX}")
}

/// A member's label and the id it follows; `None` for any other name.
pub fn parse_chain_name<K: FromStr>(name: &str) -> Option<(&str, K)> {
    let (label, prev) = name.strip_suffix(SUFFIX)?.rsplit_once(SEPARATOR)?;
    Some((label, prev.parse().ok()?))
}

/// What a reader calls a blob: a member's label, else the name itself (a
/// loose blob is put under its label).
pub fn label_of(name: &str) -> &str {
    parse_chain_name::<u64>(name).map_or(name, |(label, _)| label)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_roundtrip_and_foreign_names_do_not_parse() {
        assert_eq!(chain_name("log-batch-3", 41), "log-batch-3~41.bin");
        assert_eq!(parse_chain_name::<i64>("log-batch-3~41.bin"), Some(("log-batch-3", 41)));
        assert_eq!(parse_chain_name::<u64>("a~b~7.bin"), Some(("a~b", 7)));
        for foreign in ["ph-00.bin", "photo.jpg", "x~y.bin", "log-claim-1~root.bin", "chain-anchor.bin"] {
            assert_eq!(parse_chain_name::<i64>(foreign), None, "{foreign}");
        }
        assert_eq!(label_of("log-claim-2~9.bin"), "log-claim-2");
        assert_eq!(label_of("snapshot-1-5.snap"), "snapshot-1-5.snap");
    }
}
