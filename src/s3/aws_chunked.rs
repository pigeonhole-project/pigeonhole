//! Decode AWS SigV4 streaming `aws-chunked` request bodies.
use anyhow::{bail, Result};

/// Strip `aws-chunked` framing into raw payload bytes.
/// Supports optional chunk extensions (`;chunk-signature=...`) and trailing headers after the
/// terminating `0` chunk.
pub fn decode_aws_chunked(input: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    loop {
        let line_end = find_crlf(input, i).ok_or_else(|| anyhow::anyhow!("truncated chunk header"))?;
        let header = std::str::from_utf8(&input[i..line_end])
            .map_err(|_| anyhow::anyhow!("non-utf8 chunk header"))?;
        let size_hex = header.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| anyhow::anyhow!("bad chunk size: {size_hex}"))?;
        i = line_end + 2;
        if size == 0 {
            // Optional trailers until blank line; ignore.
            break;
        }
        if i + size + 2 > input.len() {
            bail!("truncated chunk body");
        }
        out.extend_from_slice(&input[i..i + size]);
        i += size;
        if input.get(i..i + 2) != Some(b"\r\n") {
            bail!("missing chunk CRLF");
        }
        i += 2;
    }
    Ok(out)
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

pub fn is_aws_chunked(headers: &axum::http::HeaderMap) -> bool {
    let enc = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if enc.split(',').any(|p| p.trim().eq_ignore_ascii_case("aws-chunked")) {
        return true;
    }
    let sha = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    sha.starts_with("STREAMING-")
}
