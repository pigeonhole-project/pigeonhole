use bytes::Bytes;
use s3gram_blob::testkit::run_conformance;
use s3gram_blob::{BlobBackend, ChatLimiter, ChatLimiterConfig, PutHint};
use s3gram_discord::{DiscordBlobStore, DiscordClient};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CHANNEL: &str = "999888777";

struct MockState {
    next_msg: AtomicU64,
    blobs: Mutex<HashMap<u64, Vec<u8>>>,
    cdn_hits: Mutex<HashMap<u64, u32>>,
    stale_once: Mutex<HashMap<u64, bool>>,
    deleted_msgs: Mutex<HashMap<u64, ()>>,
}

fn extract_multipart_payload(body: &[u8]) -> Vec<u8> {
    let marker = b"Content-Disposition: form-data; name=\"files[0]\";";
    let start = body
        .windows(marker.len())
        .position(|w| w == marker)
        .unwrap_or(0);
    let tail = &body[start..];
    let Some(hdr) = tail.windows(4).position(|w| w == b"\r\n\r\n") else {
        return body.to_vec();
    };
    let data = &tail[hdr + 4..];
    if let Some(end) = data.windows(4).position(|w| w == b"\r\n--") {
        return data[..end].to_vec();
    }
    data.to_vec()
}

#[tokio::test]
async fn wiremock_conformance_suite() {
    let server = MockServer::start().await;
    let server_uri = server.uri();
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(10_000),
        blobs: Mutex::new(HashMap::new()),
        cdn_hits: Mutex::new(HashMap::new()),
        stale_once: Mutex::new(HashMap::new()),
        deleted_msgs: Mutex::new(HashMap::new()),
    });

    let st = state.clone();
    let api_base = format!("{server_uri}/api/v10");
    let uri_for_post = server_uri.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages"))
        .respond_with(move |req: &wiremock::Request| {
            let blob = extract_multipart_payload(&req.body);
            let mid = st.next_msg.fetch_add(1, Ordering::SeqCst);
            let aid = mid * 10;
            st.blobs.lock().unwrap().insert(aid, blob);
            let url = format!("{uri_for_post}/cdn/{aid}");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": mid.to_string(),
                "attachments": [{
                    "id": aid.to_string(),
                    "url": url,
                    "filename": "a.bin"
                }]
            }))
        })
        .mount(&server)
        .await;

    let st_refresh = state.clone();
    let uri_for_refresh = server_uri.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/messages/[0-9]+"))
        .respond_with(move |req: &wiremock::Request| {
            let path = req.url.path();
            let mid: u64 = path.rsplit('/').next().unwrap_or("0").parse().unwrap_or(0);
            let aid = mid * 10;
            if !st_refresh.blobs.lock().unwrap().contains_key(&aid) {
                return ResponseTemplate::new(404);
            }
            let url = format!("{uri_for_refresh}/cdn/{aid}/fresh");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": mid.to_string(),
                "attachments": [{
                    "id": aid.to_string(),
                    "url": url,
                    "filename": "a.bin"
                }]
            }))
        })
        .mount(&server)
        .await;

    let st = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/cdn/[0-9]+(/fresh)?"))
        .respond_with(move |req: &wiremock::Request| {
            let path = req.url.path();
            let aid: u64 = path
                .trim_start_matches("/cdn/")
                .trim_end_matches("/fresh")
                .parse()
                .unwrap_or(0);
            let mut hits = st.cdn_hits.lock().unwrap();
            *hits.entry(aid).or_insert(0) += 1;
            let count = hits[&aid];

            let mut stale = st.stale_once.lock().unwrap();
            if path.ends_with("/fresh") {
                stale.insert(aid, false);
            } else if !stale.contains_key(&aid) {
                stale.insert(aid, true);
                if count == 1 {
                    return ResponseTemplate::new(403);
                }
            }

            let Some(data) = st.blobs.lock().unwrap().get(&aid).cloned() else {
                return ResponseTemplate::new(404);
            };
            ResponseTemplate::new(200).set_body_raw(data, "application/octet-stream")
        })
        .mount(&server)
        .await;

    let st_del = state.clone();
    Mock::given(method("DELETE"))
        .and(path_regex(r"/api/v10/channels/.+/messages/[0-9]+"))
        .respond_with(move |req: &wiremock::Request| {
            let path = req.url.path();
            let mid: u64 = path.rsplit('/').next().unwrap_or("0").parse().unwrap_or(0);
            let aid = mid * 10;
            let mut deleted = st_del.deleted_msgs.lock().unwrap();
            if deleted.contains_key(&mid) {
                return ResponseTemplate::new(404);
            }
            deleted.insert(mid, ());
            st_del.blobs.lock().unwrap().remove(&aid);
            ResponseTemplate::new(204)
        })
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v10/channels/999888777/permissions/@me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "permissions": "2147567616"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v10/users/@me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "42"
        })))
        .mount(&server)
        .await;

    let dc = DiscordClient::with_api_base("test-token".into(), api_base).unwrap();
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig {
        send_rate_per_sec: 1000.0,
        send_burst: 100.0,
        get_file_rate_per_sec: 1000.0,
        get_file_burst: 100.0,
        delete_rate_per_sec: 1000.0,
        delete_burst: 100.0,
        upload_concurrency: 8,
        download_concurrency: 8,
    }));
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), limiter, None);
    run_conformance(&store).await.unwrap();
}

#[tokio::test]
async fn wiremock_expired_url_refresh_and_429_retry() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());

    // First POST 429, then success.
    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "0.05")
                .set_body_json(serde_json::json!({"retry_after": 0.05, "message": "slow down"})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "555",
            "attachments": [{
                "id": "5550",
                "url": format!("{}/cdn/stale", server.uri()),
                "filename": "x.bin"
            }]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/cdn/stale"))
        .respond_with(ResponseTemplate::new(403))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/cdn/fresh"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(b"payload-bytes", "application/octet-stream"),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/messages/555"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "555",
            "attachments": [{
                "id": "5550",
                "url": format!("{}/cdn/fresh", server.uri()),
                "filename": "x.bin"
            }]
        })))
        .mount(&server)
        .await;

    let dc = DiscordClient::with_api_base("test-token".into(), api_base).unwrap();
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig::default()));
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), limiter, None);

    let loc = store
        .put(
            Bytes::from_static(b"payload-bytes"),
            PutHint::new("x.bin", ""),
        )
        .await
        .unwrap();
    let got = s3gram_blob::collect_stream(store.get(&loc, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(got.as_ref(), b"payload-bytes");
}
