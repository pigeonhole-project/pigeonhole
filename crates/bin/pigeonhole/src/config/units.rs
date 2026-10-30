//! Sizes in the config: a number of bytes or a string with a binary unit
//! (`"1 MiB"`, `"512KiB"`; bytesize). Decimal units (`KB`, `MB`, `GB`, …, and
//! their one-letter forms `k`, `m`, `g`) are refused: `"20MB"` is 20·10⁶
//! bytes, under Telegram's 20 MiB by 5%, and nobody means that by it.
//! Durations are humantime strings (`"45s"`, `"10m"`, `"7d"`).
//!
//! A knob that can be switched off takes `"off"` (`None`), and refuses zero:
//! a zero timeout or interval reads as "at once" as much as "never".

use bytesize::{ByteSize, Unit};
use humantime_serde::re::humantime;
use serde::de::{self, Deserialize, Deserializer, Visitor};
use std::fmt;
use std::time::Duration;

/// What switches a knob off.
pub(crate) const OFF: &str = "off";

/// `#[serde(deserialize_with)]` for a size in bytes.
pub(crate) fn size<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    d.deserialize_any(SizeVisitor { off: false })
}

/// A size, or `"off"`: `None`.
pub(crate) fn size_or_off<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    d.deserialize_any(SizeVisitor { off: true }).map(|n| (n > 0).then_some(n))
}

/// A count, or `"off"`: `None`.
pub(crate) fn count_or_off<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    d.deserialize_any(CountVisitor)
}

/// A duration, or `"off"`: `None`.
pub(crate) fn duration_or_off<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    let text = String::deserialize(d)?;
    if text.trim() == OFF {
        return Ok(None);
    }
    match humantime::parse_duration(&text).map_err(|e| de::Error::custom(format!("{text:?}: {e}")))? {
        Duration::ZERO => Err(de::Error::custom(format!("{text:?}: zero is ambiguous here, write \"{OFF}\""))),
        d => Ok(Some(d)),
    }
}

/// [`duration_or_off`] of an optional field (with `#[serde(default)]`):
/// `None` when absent, `Some(None)` when off.
pub(crate) fn opt_duration_or_off<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<Duration>>, D::Error> {
    duration_or_off(d).map(Some)
}

/// [`size`] of an optional field (with `#[serde(default)]`).
pub(crate) fn opt_size<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    size(d).map(Some)
}

/// `"1 MiB"` → 1048576; a decimal or unknown unit is an error.
pub(crate) fn parse_size(text: &str) -> Result<u64, String> {
    let s = text.trim();
    let number = s.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(s.len());
    let unit = s[number..].trim_start();
    if !unit.is_empty() {
        match unit.parse::<Unit>() {
            Ok(
                Unit::KiloByte
                | Unit::MegaByte
                | Unit::GigaByte
                | Unit::TeraByte
                | Unit::PetaByte
                | Unit::ExaByte,
            ) => return Err(format!("{text:?}: decimal units are refused, write KiB, MiB, GiB")),
            Ok(_) => {}
            Err(_) => return Err(format!("{text:?}: unknown unit (B, KiB, MiB, GiB, TiB)")),
        }
    }
    s.parse::<ByteSize>().map(|b| b.as_u64()).map_err(|e| format!("{text:?}: {e}"))
}

fn zero_is_off<E: de::Error>() -> E {
    E::custom(format!("zero is ambiguous here, write \"{OFF}\""))
}

struct SizeVisitor {
    /// Takes `"off"`, as 0, and refuses a 0 written out.
    off: bool,
}

impl<'de> Visitor<'de> for SizeVisitor {
    type Value = usize;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a size: bytes, or a string like \"1 MiB\"")?;
        if self.off {
            write!(f, ", or \"{OFF}\"")?;
        }
        Ok(())
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<usize, E> {
        if self.off && v == 0 {
            return Err(zero_is_off());
        }
        usize::try_from(v).map_err(E::custom)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<usize, E> {
        self.visit_u64(u64::try_from(v).map_err(|_| E::custom(format!("size {v} is negative")))?)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<usize, E> {
        if self.off && v.trim() == OFF {
            return Ok(0);
        }
        self.visit_u64(parse_size(v).map_err(E::custom)?)
    }
}

struct CountVisitor;

impl<'de> Visitor<'de> for CountVisitor {
    type Value = Option<u64>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a positive number, or \"{OFF}\"")
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<u64>, E> {
        match v {
            0 => Err(zero_is_off()),
            v => Ok(Some(v)),
        }
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Option<u64>, E> {
        self.visit_u64(u64::try_from(v).map_err(|_| E::custom(format!("{v} is negative")))?)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<u64>, E> {
        match v.trim() {
            OFF => Ok(None),
            _ => Err(E::invalid_value(de::Unexpected::Str(v), &self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn binary_units_only() {
        for (text, want) in [
            ("1024", 1024),
            ("0", 0),
            ("1 MiB", 1 << 20),
            ("19MiB", 19 << 20),
            ("512 KiB", 512 << 10),
            ("1.5 GiB", 3 << 29),
            ("2 gib", 2 << 30),
            ("64 B", 64),
        ] {
            assert_eq!(parse_size(text), Ok(want), "{text}");
        }
        for text in ["10MB", "10 mb", "1 KB", "4k", "1M", "2 GB", "1 TB"] {
            let err = parse_size(text).unwrap_err();
            assert!(err.contains("decimal units are refused"), "{text}: {err}");
        }
        for text in ["1 MiBs", "lots", "1 kibibyte"] {
            assert!(parse_size(text).is_err(), "{text} accepted");
        }
    }
}
