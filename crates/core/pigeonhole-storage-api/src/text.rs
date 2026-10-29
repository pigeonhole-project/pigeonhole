//! Small blobs as text messages: a chat hands the text out with the message
//! (a listing of 100 messages is 100 blobs read), no upload, no download.
//!
//! A message is `<name> <len>\n<payload>`: the payload packs 15 bits per
//! character into U+20000..U+27FFF (CJK Extension B, all assigned, outside
//! the BMP). Telegram (4096) and Discord (2000) both count a message's limit
//! in code points, strip whitespace at the ends, and keep these characters
//! exactly (live checks: docs/telegram-mtproto.md § Text messages,
//! docs/discord-log.md § Text messages). The encoding has no whitespace but
//! the header's one space and newline.

use bytes::Bytes;

const BASE: u32 = 0x20000;
const BITS: u32 = 15;

/// Characters the payload of `len` bytes takes.
fn payload_chars(len: usize) -> usize {
    (len * 8).div_ceil(BITS as usize)
}

fn header(name: &str, len: usize) -> String {
    format!("{name} {len}\n")
}

/// Whether a blob named `name` of `len` bytes fits one message of
/// `max_chars` characters.
pub fn fits(name: &str, len: usize, max_chars: usize) -> bool {
    len > 0
        && !name.is_empty()
        && name.chars().all(|c| c.is_ascii_graphic())
        && header(name, len).chars().count() + payload_chars(len) <= max_chars
}

/// The text of blob `name`: see [`fits`] first.
pub fn encode(name: &str, data: &[u8]) -> String {
    let mut out = header(name, data.len());
    out.reserve(payload_chars(data.len()) * 4);
    let (mut acc, mut nbits) = (0u32, 0u32);
    for &b in data {
        acc = (acc << 8) | b as u32;
        nbits += 8;
        if nbits >= BITS {
            nbits -= BITS;
            out.push(char::from_u32(BASE + (acc >> nbits)).unwrap());
            acc &= (1 << nbits) - 1;
        }
    }
    if nbits > 0 {
        out.push(char::from_u32(BASE + (acc << (BITS - nbits))).unwrap());
    }
    out
}

/// Name and bytes of a text written by [`encode`]; `None` for any other text.
pub fn decode(text: &str) -> Option<(String, Bytes)> {
    let (head, payload) = text.split_once('\n')?;
    let (name, len) = head.rsplit_once(' ')?;
    let len: usize = len.parse().ok()?;
    if name.is_empty() || payload.chars().count() != payload_chars(len) {
        return None;
    }
    let mut out = Vec::with_capacity(len);
    let (mut acc, mut nbits) = (0u32, 0u32);
    for c in payload.chars() {
        let v = (c as u32).checked_sub(BASE).filter(|v| *v < 1 << BITS)?;
        acc = (acc << BITS) | v;
        nbits += BITS;
        while nbits >= 8 && out.len() < len {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
            acc &= (1 << nbits) - 1;
        }
    }
    (out.len() == len).then(|| (name.to_string(), Bytes::from(out)))
}

/// The name of a text written by [`encode`], without decoding the payload.
pub fn name_of(text: &str) -> Option<&str> {
    let (head, _) = text.split_once('\n')?;
    let (name, len) = head.rsplit_once(' ')?;
    len.parse::<usize>().ok()?;
    Some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_every_length_and_byte() {
        for len in [1usize, 2, 14, 15, 16, 17, 30, 255, 1000] {
            let data: Vec<u8> = (0..len).map(|i| (i * 131 + 7) as u8).collect();
            let text = encode("ph-ab.bin", &data);
            assert_eq!(text.matches([' ', '\t', '\r']).count(), 1, "only the header's space");
            assert_eq!(decode(&text), Some(("ph-ab.bin".to_string(), Bytes::from(data))), "{len}");
            assert_eq!(name_of(&text), Some("ph-ab.bin"));
        }
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode("x", &all)).unwrap().1.as_ref(), &all[..]);
    }

    #[test]
    fn the_limit_is_counted_in_characters() {
        let name = "ph-0123456789abcdef0123456789abcdef.bin";
        for (limit, at_least) in [(4096, 7500), (2000, 3600)] {
            let mut max = 0;
            while fits(name, max + 1, limit) {
                max += 1;
            }
            assert!(max > at_least, "{limit}: {max}");
            assert!(encode(name, &vec![0xa5; max]).chars().count() <= limit);
        }
        assert!(!fits(name, 0, 4096), "empty blobs are refused anyway");
        assert!(!fits("a name", 10, 4096), "a name with a space");
    }

    #[test]
    fn foreign_texts_do_not_decode() {
        for t in ["hello", "x 3\nabc", "x 2\n", "\n", "x 99999999999999999999\n"] {
            assert_eq!(decode(t), None, "{t:?}");
        }
    }
}
