//! How a chat store (Telegram, Discord) names its chain members: a member
//! put after `prev` is named `<label>~<prev>.bin`, so a reader learns what
//! it follows from the name alone, readable without the data (a file name,
//! a text message's first line); the first member of a chain is named
//! `<label>~root.bin` (docs/chain-log.md).

use std::fmt::Display;
use std::str::FromStr;

/// What every member's name ends with.
const SUFFIX: &str = ".bin";
/// Between the label and `prev`.
const SEPARATOR: char = '~';
/// `prev` of the first member.
const ROOT: &str = "root";

/// The name of a member labelled `label` put after `prev` (`None`: the
/// first member).
pub fn chain_name<K: Display>(label: &str, prev: Option<K>) -> String {
    match prev {
        Some(p) => format!("{label}{SEPARATOR}{p}{SUFFIX}"),
        None => format!("{label}{SEPARATOR}{ROOT}{SUFFIX}"),
    }
}

/// A member's label and the id it follows (`None`: the first member);
/// `None` for any other name.
pub fn parse_chain_name<K: FromStr>(name: &str) -> Option<(&str, Option<K>)> {
    let (label, prev) = name.strip_suffix(SUFFIX)?.rsplit_once(SEPARATOR)?;
    match prev {
        ROOT => Some((label, None)),
        p => Some((label, Some(p.parse().ok()?))),
    }
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
        assert_eq!(chain_name("log-batch-3", Some(41)), "log-batch-3~41.bin");
        assert_eq!(parse_chain_name::<i64>("log-batch-3~41.bin"), Some(("log-batch-3", Some(41))));
        assert_eq!(parse_chain_name::<u64>("a~b~7.bin"), Some(("a~b", Some(7))));
        assert_eq!(parse_chain_name::<i64>(&chain_name::<i64>("log-claim-1", None)), Some(("log-claim-1", None)));
        for foreign in ["ph-00.bin", "photo.jpg", "x~y.bin", "chain-probe.bin"] {
            assert_eq!(parse_chain_name::<i64>(foreign), None, "{foreign}");
        }
        assert_eq!(label_of("log-claim-2~9.bin"), "log-claim-2");
        assert_eq!(label_of("snapshot-1-5.snap"), "snapshot-1-5.snap");
    }
}
