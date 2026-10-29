//! S3 additional checksums (`x-amz-checksum-*`): what a write expects, the
//! hasher that computes them as its body streams in, and how they are stored.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use s3s::checksum::ChecksumHasher;
use s3s::dto::{Checksum, ChecksumAlgorithm};
use s3s::{s3_error, S3Result, TrailingHeaders};
use std::sync::{Arc, Mutex};

/// Copy every checksum field between two s3s types that name them alike
/// (an input, a [`Checksum`], an output).
macro_rules! copy_checksums {
    ($from:expr => $to:expr) => {{
        $to.checksum_crc32 = $from.checksum_crc32.clone();
        $to.checksum_crc32c = $from.checksum_crc32c.clone();
        $to.checksum_sha1 = $from.checksum_sha1.clone();
        $to.checksum_sha256 = $from.checksum_sha256.clone();
        $to.checksum_crc64nvme = $from.checksum_crc64nvme.clone();
        $to.checksum_sha512 = $from.checksum_sha512.clone();
        $to.checksum_md5 = $from.checksum_md5.clone();
        $to.checksum_xxhash64 = $from.checksum_xxhash64.clone();
        $to.checksum_xxhash3 = $from.checksum_xxhash3.clone();
        $to.checksum_xxhash128 = $from.checksum_xxhash128.clone();
    }};
}
pub(crate) use copy_checksums;

/// Every checksum by name: `x-amz-checksum-<name>`, its key in the stored
/// JSON, and, upper-cased, its algorithm.
fn fields(c: &Checksum) -> [(&'static str, &Option<String>); 10] {
    [
        ("crc32", &c.checksum_crc32),
        ("crc32c", &c.checksum_crc32c),
        ("sha1", &c.checksum_sha1),
        ("sha256", &c.checksum_sha256),
        ("crc64nvme", &c.checksum_crc64nvme),
        ("sha512", &c.checksum_sha512),
        ("md5", &c.checksum_md5),
        ("xxhash64", &c.checksum_xxhash64),
        ("xxhash3", &c.checksum_xxhash3),
        ("xxhash128", &c.checksum_xxhash128),
    ]
}

fn fields_mut(c: &mut Checksum) -> [(&'static str, &mut Option<String>); 10] {
    [
        ("crc32", &mut c.checksum_crc32),
        ("crc32c", &mut c.checksum_crc32c),
        ("sha1", &mut c.checksum_sha1),
        ("sha256", &mut c.checksum_sha256),
        ("crc64nvme", &mut c.checksum_crc64nvme),
        ("sha512", &mut c.checksum_sha512),
        ("md5", &mut c.checksum_md5),
        ("xxhash64", &mut c.checksum_xxhash64),
        ("xxhash3", &mut c.checksum_xxhash3),
        ("xxhash128", &mut c.checksum_xxhash128),
    ]
}

fn enable(hasher: &mut ChecksumHasher, algorithm: &str) -> S3Result<()> {
    match algorithm {
        ChecksumAlgorithm::CRC32 => hasher.crc32 = Some(Default::default()),
        ChecksumAlgorithm::CRC32C => hasher.crc32c = Some(Default::default()),
        ChecksumAlgorithm::SHA1 => hasher.sha1 = Some(Default::default()),
        ChecksumAlgorithm::SHA256 => hasher.sha256 = Some(Default::default()),
        ChecksumAlgorithm::CRC64NVME => hasher.crc64nvme = Some(Default::default()),
        ChecksumAlgorithm::SHA512 => hasher.sha512 = Some(Default::default()),
        ChecksumAlgorithm::MD5 => hasher.md5 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH64 => hasher.xxhash64 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH3 => hasher.xxhash3 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH128 => hasher.xxhash128 = Some(Default::default()),
        _ => return Err(s3_error!(NotImplemented, "Unsupported checksum algorithm")),
    }
    Ok(())
}

fn is_active(h: &ChecksumHasher) -> bool {
    h.crc32.is_some()
        || h.crc32c.is_some()
        || h.sha1.is_some()
        || h.sha256.is_some()
        || h.crc64nvme.is_some()
        || h.sha512.is_some()
        || h.md5.is_some()
        || h.xxhash64.is_some()
        || h.xxhash3.is_some()
        || h.xxhash128.is_some()
}

/// The checksums one write carries and the hasher that checks them.
pub(crate) struct IncomingChecksums {
    expected: Checksum,
    /// `None`: nothing to compute.
    hasher: Option<Arc<Mutex<ChecksumHasher>>>,
}

impl IncomingChecksums {
    /// Computes each checksum `expected` names and each of `algorithms`.
    pub(crate) fn new(expected: Checksum, algorithms: &[Option<&str>]) -> S3Result<Self> {
        let mut hasher = ChecksumHasher::default();
        for (name, value) in fields(&expected) {
            if value.is_some() {
                enable(&mut hasher, &name.to_ascii_uppercase())?;
            }
        }
        for algorithm in algorithms.iter().flatten() {
            enable(&mut hasher, algorithm)?;
        }
        let hasher = is_active(&hasher).then(|| Arc::new(Mutex::new(hasher)));
        Ok(Self { expected, hasher })
    }

    /// `body`, every frame of it fed to the hasher.
    pub(crate) fn wrap<S>(&self, body: S) -> impl Stream<Item = anyhow::Result<Bytes>> + Unpin
    where
        S: Stream<Item = anyhow::Result<Bytes>> + Unpin,
    {
        let hasher = self.hasher.clone();
        body.map(move |item| {
            let b = item?;
            if let Some(h) = &hasher {
                h.lock().unwrap_or_else(|e| e.into_inner()).update(&b);
            }
            Ok(b)
        })
    }

    /// After the body: checksums sent as trailers join those from the
    /// headers, and each must match what was computed. The computed ones, or
    /// `None` if nothing was.
    pub(crate) fn verify(mut self, trailers: Option<TrailingHeaders>) -> S3Result<Option<Checksum>> {
        if let Some(trailers) = trailers.and_then(|t| t.take()) {
            merge_trailers(&mut self.expected, &trailers)?;
        }
        let computed = match &self.hasher {
            Some(h) => std::mem::take(&mut *h.lock().unwrap_or_else(|e| e.into_inner())).finalize(),
            None => Checksum::default(),
        };
        let actual = fields(&computed);
        for (i, (name, want)) in fields(&self.expected).into_iter().enumerate() {
            if want.is_some() && actual[i].1 != want {
                return Err(s3_error!(BadDigest, "checksum_{name} mismatch"));
            }
        }
        Ok(self.hasher.is_some().then_some(computed))
    }
}

fn merge_trailers(expected: &mut Checksum, trailers: &http::HeaderMap) -> S3Result<()> {
    for (name, slot) in fields_mut(expected) {
        if let Some(v) = trailers.get(format!("x-amz-checksum-{name}")) {
            let v = v.to_str().map_err(|_| s3_error!(InvalidArgument, "Invalid trailer checksum"))?;
            *slot = Some(v.to_owned());
        }
    }
    Ok(())
}

/// The stored form: `{"<name>": "<base64>", ...}`.
pub(crate) fn to_json(c: &Checksum) -> String {
    let map: serde_json::Map<String, serde_json::Value> = fields(c)
        .into_iter()
        .filter_map(|(name, v)| Some((name.to_string(), serde_json::Value::String(v.clone()?))))
        .collect();
    serde_json::Value::Object(map).to_string()
}

pub(crate) fn from_json(s: &str) -> Checksum {
    let mut c = Checksum::default();
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(s) else {
        return c;
    };
    for (name, slot) in fields_mut(&mut c) {
        *slot = map.get(name).and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(str::to_owned);
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_named_checksum_is_an_algorithm() {
        for (name, _) in fields(&Checksum::default()) {
            enable(&mut ChecksumHasher::default(), &name.to_ascii_uppercase()).unwrap();
        }
    }

    #[test]
    fn the_stored_form_round_trips() {
        let mut c = Checksum::default();
        for (i, (_, slot)) in fields_mut(&mut c).into_iter().enumerate() {
            *slot = Some(format!("v{i}"));
        }
        let back = from_json(&to_json(&c));
        assert_eq!(fields(&back).map(|(_, v)| v.clone()), fields(&c).map(|(_, v)| v.clone()));
        assert_eq!(to_json(&Checksum::default()), "{}");
    }

    #[tokio::test]
    async fn a_wrong_checksum_is_named() {
        let body = || futures::stream::iter([Ok(Bytes::from_static(b"hello"))]);
        let expected = Checksum { checksum_crc32: Some("NhCmhg==".into()), ..Default::default() };
        let ok = IncomingChecksums::new(expected, &[]).unwrap();
        ok.wrap(body()).for_each(|_| async {}).await;
        assert_eq!(ok.verify(None).unwrap().unwrap().checksum_crc32.as_deref(), Some("NhCmhg=="));

        let expected = Checksum { checksum_crc32: Some("AAAAAA==".into()), ..Default::default() };
        let bad = IncomingChecksums::new(expected, &[]).unwrap();
        bad.wrap(body()).for_each(|_| async {}).await;
        assert!(bad.verify(None).unwrap_err().to_string().contains("checksum_crc32 mismatch"));

        let none = IncomingChecksums::new(Checksum::default(), &[None]).unwrap();
        assert!(none.verify(None).unwrap().is_none());
    }
}
