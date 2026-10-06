//! Streaming decoder for AWS SigV4 `aws-chunked` request bodies.
//!
//! Chunk-signature extensions and trailing checksum headers are accepted but not
//! cryptographically verified (would need the SigV4 signing key at decode time).
use anyhow::{bail, Result};

/// Incremental aws-chunked framer → raw payload bytes.
#[derive(Debug, Default)]
pub struct AwsChunkedDecoder {
    buf: Vec<u8>,
    /// Remaining bytes of the current chunk body, if mid-body.
    body_left: Option<usize>,
    done: bool,
}

impl AwsChunkedDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finished(&self) -> bool {
        self.done
    }

    /// Push network bytes; returns newly decoded payload bytes (may be empty).
    pub fn push(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        if self.done {
            // Ignore trailing junk after the terminating chunk.
            return Ok(Vec::new());
        }
        self.buf.extend_from_slice(input);
        let mut out = Vec::new();
        loop {
            if let Some(left) = self.body_left {
                if left == 0 {
                    // Expect CRLF after chunk body.
                    if self.buf.len() < 2 {
                        break;
                    }
                    if &self.buf[..2] != b"\r\n" {
                        bail!("missing chunk CRLF");
                    }
                    self.buf.drain(..2);
                    self.body_left = None;
                    continue;
                }
                let n = left.min(self.buf.len());
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&self.buf[..n]);
                self.buf.drain(..n);
                self.body_left = Some(left - n);
                continue;
            }

            let Some(line_end) = find_crlf(&self.buf, 0) else {
                break;
            };
            let header = std::str::from_utf8(&self.buf[..line_end])
                .map_err(|_| anyhow::anyhow!("non-utf8 chunk header"))?;
            let size_hex = header.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(size_hex, 16)
                .map_err(|_| anyhow::anyhow!("bad chunk size: {size_hex}"))?;
            self.buf.drain(..line_end + 2);
            if size == 0 {
                self.done = true;
                // Drop optional trailers; caller stops feeding.
                self.buf.clear();
                break;
            }
            self.body_left = Some(size);
        }
        Ok(out)
    }

    /// Flush: error if the stream ended mid-chunk.
    pub fn finish(self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        if self.body_left.is_some() || !self.buf.is_empty() {
            bail!("truncated aws-chunked stream");
        }
        bail!("aws-chunked stream ended without terminating chunk");
    }
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
    if enc
        .split(',')
        .any(|p| p.trim().eq_ignore_ascii_case("aws-chunked"))
    {
        return true;
    }
    let sha = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    sha.starts_with("STREAMING-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_roundtrip() {
        let framed = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut d = AwsChunkedDecoder::new();
        let mut out = Vec::new();
        for piece in framed.chunks(3) {
            out.extend(d.push(piece).unwrap());
        }
        d.finish().unwrap();
        assert_eq!(out, b"hello world");
    }
}
