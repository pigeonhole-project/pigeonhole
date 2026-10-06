use crate::config::Config;
use crate::s3::error::S3Error;
use axum::http::Request;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

pub fn authorize<B>(cfg: &Config, req: &Request<B>) -> Result<(), S3Error> {
    // Allow unsigned local health probes
    if req.headers().get("authorization").is_none()
        && req.headers().get("x-amz-content-sha256").is_none()
    {
        // Still require credentials for aws cli compatibility path — but for demo
        // accept missing auth only if S3GRAM_INSECURE=1
        if std::env::var("S3GRAM_INSECURE").ok().as_deref() == Some("1") {
            return Ok(());
        }
    }

    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(S3Error::access_denied)?;

    if !auth.starts_with("AWS4-HMAC-SHA256 ") {
        return Err(S3Error::access_denied());
    }

    let rest = &auth["AWS4-HMAC-SHA256 ".len()..];
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in rest.split(", ") {
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }

    let credential = credential.ok_or_else(S3Error::access_denied)?;
    let signed_headers = signed_headers.ok_or_else(S3Error::access_denied)?;
    let signature = signature.ok_or_else(S3Error::access_denied)?;

    let mut cred_parts = credential.split('/');
    let access_key = cred_parts.next().unwrap_or("");
    let date_stamp = cred_parts.next().unwrap_or("");
    let region = cred_parts.next().unwrap_or("");
    let service = cred_parts.next().unwrap_or("");
    let term = cred_parts.next().unwrap_or("");

    if access_key != cfg.access_key || service != "s3" || term != "aws4_request" {
        return Err(S3Error::access_denied());
    }

    let amz_date = req
        .headers()
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(S3Error::access_denied)?;

    let payload_hash = req
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let canonical_uri = canonical_uri(req.uri().path());
    let canonical_query = canonical_query_string(req.uri().query().unwrap_or(""));

    let mut headers_map = BTreeMap::new();
    for name in signed_headers.split(';') {
        let value = if name == "host" {
            req.headers()
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string()
        } else {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .trim()
                .to_string()
        };
        headers_map.insert(name.to_string(), value);
    }

    let mut canonical_headers = String::new();
    for (k, v) in &headers_map {
        canonical_headers.push_str(k);
        canonical_headers.push(':');
        canonical_headers.push_str(v);
        canonical_headers.push('\n');
    }

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
    );

    let hashed_request = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashed_request}");

    let signing_key = derive_signing_key(&cfg.secret_key, date_stamp, region, service);
    let expected = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    if expected != signature {
        return Err(S3Error::signature_mismatch());
    }
    Ok(())
}

/// URI-encode path per AWS SigV4 (encode each segment, keep `/`).
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    path.split('/')
        .map(|seg| uri_encode(&percent_decode(seg)))
        .collect::<Vec<_>>()
        .join("/")
}

fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    // Wire query is already percent-encoded; decode then re-encode for SigV4
    // so values like `%2F` become `/` → `%2F` instead of `%252F`.
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut it = p.splitn(2, '=');
            let k = percent_decode(it.next().unwrap_or(""));
            let v = percent_decode(it.next().unwrap_or(""));
            (uri_encode(&k), uri_encode(&v))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn uri_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
